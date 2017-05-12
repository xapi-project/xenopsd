open Stdext.Threadext
open Stdext
module D = Debug.Make(struct let name = "xapi_work_queues" end)
open D

module StringMap = Map.Make(struct type t = string let compare = compare end)

let push_with_coalesce should_keep item queue =
  (* [filter_with_memory p xs] returns elements [x \in xs] where [p (x_i, [x_0...x_i-1])] *)
  let filter_with_memory p xs =
    List.fold_left (fun (acc, xs) x -> xs :: acc, x :: xs) ([], []) xs
    |> fst |> List.rev |> List.combine xs (* association list of (element, all previous elements) *)
    |> List.filter p
    |> List.map fst in

  let to_list queue = Queue.fold (fun xs x -> x :: xs) [] queue |> List.rev in
  let of_list xs =
    let q = Queue.create () in
    List.iter (fun x -> Queue.push x q) xs;
    q in

  Queue.push item queue;
  let queue' =
    to_list queue
    |> filter_with_memory (fun (this, prev) -> should_keep this prev)
    |> of_list in
  Queue.clear queue;
  Queue.transfer queue' queue

module Queues = struct
  (** A set of queues where 'pop' operates on each queue in a round-robin fashion *)

  type tag = string
  (** Each distinct 'tag' value creates a separate virtual queue *)

  type 'a t = {
    mutable qs: 'a Queue.t StringMap.t;
    mutable last_tag: string;
    m: Mutex.t;
    c: Condition.t;
  }

  let create () = {
    qs = StringMap.empty;
    last_tag = "";
    m = Mutex.create ();
    c = Condition.create ();
  }

  let get tag qs =
    Mutex.execute qs.m
      (fun () ->
         if StringMap.mem tag qs.qs then StringMap.find tag qs.qs else Queue.create ()
      )

  let tags qs =
    Mutex.execute qs.m
      (fun () ->
         StringMap.fold (fun x _ acc -> x :: acc) qs.qs []
      )

  let get_last_tag qs =
    Mutex.execute qs.m
      (fun () ->
         qs.last_tag
      )

  let push_with_coalesce should_keep tag item qs =
    Mutex.execute qs.m
      (fun () ->
         let q = if StringMap.mem tag qs.qs then StringMap.find tag qs.qs else Queue.create () in
         push_with_coalesce should_keep item q;
         qs.qs <- StringMap.add tag q qs.qs;
         Condition.signal qs.c
      )

  let pop qs =
    Mutex.execute qs.m
      (fun () ->
         while StringMap.is_empty qs.qs do
           Condition.wait qs.c qs.m;
         done;
         (* partition based on last_tag *)
         let before, after = StringMap.partition (fun x _ -> x <= qs.last_tag) qs.qs in
         (* the min_binding in the 'after' is the next queue *)
         let last_tag, q = StringMap.min_binding (if StringMap.is_empty after then before else after) in
         qs.last_tag <- last_tag;
         let item = Queue.pop q in
         (* remove empty queues from the whole mapping *)
         qs.qs <- if Queue.is_empty q then StringMap.remove last_tag qs.qs else qs.qs;
         last_tag, item
      )

  let transfer_tag tag a b =
    Mutex.execute a.m
      (fun () ->
         Mutex.execute b.m
           (fun () ->
              if StringMap.mem tag a.qs then begin
                b.qs <- StringMap.add tag (StringMap.find tag a.qs) b.qs;
                a.qs <- StringMap.remove tag a.qs;
                Condition.signal b.c
              end
           )
      )
end

module type Item = sig
  type t
  val describe_item : t -> string
  val dump_task : t -> Rpc.t
  val execute : t -> unit
  val finally : t -> unit
  val should_keep : t -> t list -> bool
end

module type Dump = sig
  type t
  val t_of_rpc : Rpc.t -> t
  val rpc_of_t : t -> Rpc.t
  val make : unit -> t
end

module type S = sig
  type item
  module Redirector :
  sig
    type t
    module Dump : Dump
    val default : t
    val parallel_queues : t
    val push : t -> string -> item -> unit
  end
  module WorkerPool :
  sig
    module Dump : Dump
    val start : int -> unit
    val set_size : int -> unit
  end
end

module Make(I:Item) = struct
  open I
  type item = I.t
  module Redirector = struct
    type t = { queues: item Queues.t; mutex: Mutex.t }

    (* When a thread is not actively processing a queue, items are placed here: *)
    let default = { queues = Queues.create (); mutex = Mutex.create () }
    (* We create another queue only for Parallel atoms so as to avoid a situation where
       Parallel atoms can not progress because all the workers available for the
       default queue are used up by other operations depending on further Parallel
       atoms, creating a deadlock.
    *)
    let parallel_queues = { queues = Queues.create (); mutex = Mutex.create () }

    (* When a thread is actively processing a queue, items are redirected to a thread-private queue *)
    let overrides = ref StringMap.empty
    let m = Mutex.create ()

    let push t tag item =
      Debug.with_thread_associated "queue"
        (fun () ->
           Mutex.execute m
             (fun () ->
                let q, redirected = if StringMap.mem tag !overrides then StringMap.find tag !overrides, true else t.queues, false in
                debug "Queue.push %s onto %s%s:[ %s ]" (describe_item item) (if redirected then "redirected " else "") tag (String.concat ", " (List.rev (Queue.fold (fun acc b -> describe_item b :: acc) [] (Queues.get tag q))));

                Queues.push_with_coalesce should_keep tag item q
             )
        ) ()

    let pop t () =
      (* We must prevent worker threads all calling Queues.pop before we've
         successfully put the redirection in place. Otherwise we end up with
         parallel threads operating on the same VM. *)
      Mutex.execute t.mutex
        (fun () ->
           let tag, item = Queues.pop t.queues in
           Mutex.execute m
             (fun () ->
                let q = Queues.create () in
                Queues.transfer_tag tag t.queues q;
                overrides := StringMap.add tag q !overrides;
                (* All items with [tag] will enter queue [q] *)
                tag, q, item
             )
        )

    let finished t tag queue =
      Mutex.execute m
        (fun () ->
           Queues.transfer_tag tag queue t.queues;
           overrides := StringMap.remove tag !overrides
           (* All items with [tag] will enter the queues queue *)
        )

    module Dump = struct
      type q = {
        tag: string;
        items: string list
      } [@@deriving rpc]
      type t = q list [@@deriving rpc]

      let make () =
        Mutex.execute m
          (fun () ->
             let one queue =
               List.map
                 (fun t ->
                    { tag = t; items = List.rev (Queue.fold (fun acc b -> describe_item b :: acc) [] (Queues.get t queue)) }
                 ) (Queues.tags queue) in
             List.concat (List.map one (default.queues :: parallel_queues.queues :: (List.map snd (StringMap.bindings !overrides))))
          )

    end
  end

  module Worker = struct
    type state =
      | Idle
      | Processing of item
      | Shutdown_requested
      | Shutdown
    type t = {
      mutable state: state;
      mutable shutdown_requested: bool;
      m: Mutex.t;
      c: Condition.t;
      mutable t: Thread.t option;
      redirector: Redirector.t;
    }

    let get_state_locked t =
      if t.shutdown_requested
      then Shutdown_requested
      else t.state

    let get_state t =
      Mutex.execute t.m
        (fun () ->
           get_state_locked t
        )

    let join t =
      Mutex.execute t.m
        (fun () ->
           assert (t.state = Shutdown);
           Opt.iter Thread.join t.t
        )

    let is_active t =
      Mutex.execute t.m
        (fun () ->
           match get_state_locked t with
           | Idle | Processing _ -> true
           | Shutdown_requested | Shutdown -> false
        )

    let shutdown t =
      Mutex.execute t.m
        (fun () ->
           if not t.shutdown_requested then begin
             t.shutdown_requested <- true;
             true (* success *)
           end else false
        )

    let restart t =
      Mutex.execute t.m
        (fun () ->
           if t.shutdown_requested && t.state <> Shutdown then begin
             t.shutdown_requested <- false;
             true (* success *)
           end else false
        )

    let create redirector =
      let t = {
        state = Idle;
        shutdown_requested = false;
        m = Mutex.create ();
        c = Condition.create ();
        t = None;
        redirector = redirector;
      } in
      let thread = Thread.create
          (fun () ->
             while not(Mutex.execute t.m (fun () ->
                 if t.shutdown_requested then t.state <- Shutdown;
                 t.shutdown_requested
               )) do
               Mutex.execute t.m (fun () -> t.state <- Idle);
               let tag, queue, item = Redirector.pop redirector () in (* blocks here *)
               debug "Queue.pop returned %s" (describe_item item);
               Mutex.execute t.m (fun () -> t.state <- Processing item);
               begin
                 try
                   execute item
                 with e ->
                   debug "Queue caught: %s" (Printexc.to_string e)
               end;
               Redirector.finished redirector tag queue;
               (* The task must have succeeded or failed. *)
               try
                 finally item
               with e ->
                 debug "Queue finally caught: %s" (Printexc.to_string e)
             done
          ) () in
      t.t <- Some thread;
      t
  end

  module WorkerPool = struct

    (* Store references to Worker.ts here *)
    let pool = ref []
    let m = Mutex.create ()


    module Dump = struct
      type w = {
        state: string;
        task: Rpc.t option;
      } [@@deriving rpc]
      type t = w list [@@deriving rpc]
      let make () =
        Mutex.execute m
          (fun () ->
             List.map
               (fun t ->
                  match Worker.get_state t with
                  | Worker.Idle -> { state = "Idle"; task = None }
                  | Worker.Processing item -> { state = Printf.sprintf "Processing %s" (describe_item item); task = Some (dump_task item) }
                  | Worker.Shutdown_requested -> { state = "Shutdown_requested"; task = None }
                  | Worker.Shutdown -> { state = "Shutdown"; task = None }
               ) !pool
          )
    end

    (* Compute the number of active threads ie those which will continue to operate *)
    let count_active queues =
      Mutex.execute m
        (fun () ->
           (* we do not want to use = when comparing queues: queues can contain (uncomparable) functions, and we
              are only interested in comparing the equality of their static references
           *)
           List.map (fun w -> w.Worker.redirector == queues && Worker.is_active w) !pool |> List.filter (fun x -> x) |> List.length
        )

    let find_one queues f = List.fold_left (fun acc x -> acc || (x.Worker.redirector == queues && (f x))) false

    (* Clean up any shutdown threads and remove them from the master list *)
    let gc queues pool =
      List.fold_left
        (fun acc w ->
           (* we do not want to use = when comparing queues: queues can contain (uncomparable) functions, and we
              are only interested in comparing the equality of their static references
           *)
           if w.Worker.redirector == queues && Worker.get_state w = Worker.Shutdown then begin
             Worker.join w;
             acc
           end else w :: acc) [] pool

    let incr queues =
      debug "Adding a new worker to the thread pool";
      Mutex.execute m
        (fun () ->
           pool := gc queues !pool;
           if not(find_one queues Worker.restart !pool)
           then pool := (Worker.create queues) :: !pool
        )

    let decr queues =
      debug "Removing a worker from the thread pool";
      Mutex.execute m
        (fun () ->
           pool := gc queues !pool;
           if not(find_one queues Worker.shutdown !pool)
           then debug "There are no worker threads left to shutdown."
        )

    let start size =
      for i = 1 to size do
        incr Redirector.default;
        incr Redirector.parallel_queues
      done

    let set_size size =
      let inner queues =
        let active = count_active queues in
        debug "XXX active = %d" active;
        for i = 1 to max 0 (size - active) do
          incr queues
        done;
        for i = 1 to max 0 (active - size) do
          decr queues
        done
      in
      inner Redirector.default;
      inner Redirector.parallel_queues
  end
end
