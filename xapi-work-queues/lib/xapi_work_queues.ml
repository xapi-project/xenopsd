open Xapi_stdext_threads.Threadext
module D = Debug.Make(struct let name = "xapi_work_queues" end)
open D

module StringMap = Map.Make(struct type t = string let compare = compare end)

module Queues = struct
  (** A set of queues where 'pop' operates on each queue in a round-robin fashion *)

  type tag = string
  (** Each distinct 'tag' value creates a separate virtual queue *)

  type 'a t = {
    mutable qs: 'a Queue.t StringMap.t;
    mutable last_tag: tag; (* used for round-robin scheduling *)
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

  let perform_push_with_coalesce should_keep item queue =
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
    match should_keep with
    | None -> ()
    | Some should_keep ->
      let queue' =
        to_list queue
        |> filter_with_memory (fun (this, prev) -> should_keep this prev)
        |> of_list in
      Queue.clear queue;
      Queue.transfer queue' queue

  let push_with_coalesce should_keep tag item qs =
    Mutex.execute qs.m
      (fun () ->
         let q = if StringMap.mem tag qs.qs then StringMap.find tag qs.qs else Queue.create () in
         perform_push_with_coalesce should_keep item q;
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
  val describe : t -> Rpc.t
  val diagnostics : t -> Rpc.t
  val execute : t -> unit
end

module type S = sig
  type t
  type item
  val create : ?should_keep:(item -> item list -> bool) -> int -> t
  val set_size : t -> int -> unit
  val push : t -> string -> item -> unit
  val diagnostics : t list -> Rpc.t * Rpc.t
end

module Make(I:Item) = struct
  open I
  type item = I.t
  let describe_item x = x |> I.describe |> Jsonrpc.to_string
  module Redirector = struct
    type t = {
      queues: item Queues.t;
      mutex: Mutex.t; (* mutex for Queue.pop *)
      mutable overrides: item Queues.t StringMap.t;
      m: Mutex.t; (* mutex for overrides *)
      should_keep: (item -> item list -> bool) option;
    }

    let create ?should_keep () = {
      queues = Queues.create ();
      mutex = Mutex.create ();
      overrides = StringMap.empty;
      m = Mutex.create ();
      should_keep;
    }

    (* When a thread is actively processing a queue, items are redirected to a thread-private queue *)
    let push t tag item =
      Debug.with_thread_associated "queue"
        (fun () ->
           Mutex.execute t.m
             (fun () ->
                let q, redirected = if StringMap.mem tag t.overrides then StringMap.find tag t.overrides, true else t.queues, false in
                debug "Queue.push %s onto %s%s:[ %s ]" (describe_item item) (if redirected then "redirected " else "") tag (String.concat ", " (List.rev (Queue.fold (fun acc b -> describe_item b :: acc) [] (Queues.get tag q))));

                Queues.push_with_coalesce t.should_keep tag item q
             )
        ) ()

    let pop t () =
      (* We must prevent worker threads all calling Queues.pop before we've
         successfully put the redirection in place. Otherwise we end up with
         parallel threads operating on the same VM. *)
      Mutex.execute t.mutex
        (fun () ->
           let tag, item = Queues.pop t.queues in
           Mutex.execute t.m (fun () ->
               let q = Queues.create () in
               Queues.transfer_tag tag t.queues q;
               t.overrides <- StringMap.add tag q t.overrides;
               (* All items with [tag] will enter queue [q] *)
               tag, q, item
             )
        )

    let finished t tag queue =
      Mutex.execute t.m (fun () ->
          Queues.transfer_tag tag queue t.queues;
          t.overrides <- StringMap.remove tag t.overrides
          (* All items with [tag] will enter the queues queue *)
        )

    module Dump = struct
      type q = {
        tag: string;
        items: string list
      } [@@deriving rpc]
      type t = q list [@@deriving rpc]

      let make redirectors =
        let queues = List.rev_map (fun t -> t.queues) redirectors in
        let overrides = List.rev_map (fun t ->
            List.map snd (StringMap.bindings t.overrides)) redirectors |> List.concat in
        let one queue =
          List.map
            (fun t ->
               { tag = t; items = List.rev (Queue.fold (fun acc b -> describe_item b :: acc) [] (Queues.get t queue)) }
            ) (Queues.tags queue) in
        List.concat (List.map one (List.rev_append queues overrides))
    end
  end

  module Worker = struct
    type state =
      | Idle
      | Processing of item
      | Shutdown_requested
      | Shutdown
    type worker_state = {
      mutable state: state;
      mutable shutdown_requested: bool;
      m: Mutex.t;
      c: Condition.t;
      redirector: Redirector.t;
    }
    type t = worker_state * Thread.t

    let get_state_locked t =
      if t.shutdown_requested && t.state <> Shutdown
      then Shutdown_requested
      else t.state

    let get_state (t, _) =
      Mutex.execute t.m
        (fun () ->
           get_state_locked t
        )

    let join (t, thread) =
      Mutex.execute t.m
        (fun () ->
           assert (t.state = Shutdown);
           Thread.join thread
        )

    let is_active (t, _) =
      Mutex.execute t.m
        (fun () ->
           match get_state_locked t with
           | Idle | Processing _ -> true
           | Shutdown_requested | Shutdown -> false
        )

    let shutdown (t, _) =
      Mutex.execute t.m
        (fun () ->
           if not t.shutdown_requested then begin
             t.shutdown_requested <- true;
             true (* success *)
           end else false
        )

    let restart (t, _) =
      Mutex.execute t.m
        (fun () ->
           if t.shutdown_requested && t.state <> Shutdown then begin
             t.shutdown_requested <- false;
             true (* success *)
           end else false
        )

    let create redirector : t =
      let t = {
        state = Idle;
        shutdown_requested = false;
        m = Mutex.create ();
        c = Condition.create ();
        redirector;
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
             done
          ) () in
      t, thread
  end

  type t = {
    redirector: Redirector.t;
    m: Mutex.t;
    mutable pool: Worker.t list;
  }

  module Dump = struct
    type w = {
      state: string;
      task: Rpc.t option;
    } [@@deriving rpc]
    type t = w list [@@deriving rpc]
    let make t =
      Mutex.execute t.m
        (fun () ->
           List.map
             (fun t ->
                match Worker.get_state t with
                | Worker.Idle -> { state = "Idle"; task = None }
                | Worker.Processing item -> { state = Printf.sprintf "Processing %s" (describe_item item); task = Some (diagnostics item) }
                | Worker.Shutdown_requested -> { state = "Shutdown_requested"; task = None }
                | Worker.Shutdown -> { state = "Shutdown"; task = None }
             ) t.pool
        )
  end
  let diagnostics redirectors =
    Redirector.Dump.(redirectors |> List.map (fun t -> t.redirector) |> make |> rpc_of_t),
    Dump.(List.map make redirectors |> List.flatten |> rpc_of_t)

  (* Compute the number of active threads ie those which will continue to operate *)
  let count_active t =
    Mutex.execute t.m
      (fun () ->
         (* we do not want to use = when comparing queues: queues can contain (uncomparable) functions, and we
            are only interested in comparing the equality of their static references
         *)
         List.map (fun w -> Worker.is_active w) t.pool |> List.filter (fun x -> x) |> List.length
      )

  let find_one _queues f = List.fold_left (fun acc x -> acc || (f x)) false

  (* Clean up any shutdown threads and remove them from the master list *)
  let gc pool =
    List.fold_left
      (fun acc w ->
         (* we do not want to use = when comparing queues: queues can contain (uncomparable) functions, and we
            are only interested in comparing the equality of their static references
         *)
         if Worker.get_state w = Worker.Shutdown then begin
           Worker.join w;
           acc
         end else w :: acc) [] pool

  let incr t =
    debug "Adding a new worker to the thread pool";
    Mutex.execute t.m
      (fun () ->
         t.pool <- gc t.pool;
         if not(find_one t.redirector Worker.restart t.pool)
         then t.pool <- (Worker.create t.redirector) :: t.pool
      )

  let decr t =
    debug "Removing a worker from the thread pool";
    Mutex.execute t.m
      (fun () ->
         t.pool <- gc t.pool;
         if not(find_one t.redirector Worker.shutdown t.pool)
         then debug "There are no worker threads left to shutdown."
      )

  let set_size t size =
    let active = count_active t in
    debug "XXX active = %d" active;
    for _ = 1 to max 0 (size - active) do
      incr t
    done;
    for _ = 1 to max 0 (active - size) do
      decr t
    done

  let create ?should_keep n =
    let t = { redirector = Redirector.create ?should_keep (); pool = []; m = Mutex.create () } in
    if n > 0 then
      set_size t n;
    t

  let push t item = Redirector.push t.redirector item
end

module Test = struct
  let tags = Alcotest.(list string)

  let test_empty_queues () =
    let qs = Queues.create () in
    let dst = Queues.create () in
    Alcotest.check tags "Newly created queue is empty" [] (Queues.tags qs);
    Queues.transfer_tag "foo" qs dst;
    Alcotest.check tags "Queue still empty after noop transfer" [] (Queues.tags qs);
    Alcotest.check tags "Destination is empty after noop transfer" [] (Queues.tags dst);
    Alcotest.check Alcotest.bool "Queue for non-existent tag is empty" true (Queues.get "foo" qs |> Queue.is_empty)

  let always _ _ = true

  let make_thread f =
    let created = ref false in
    let m = Mutex.create () in
    let c = Condition.create () in
    let thr = Thread.create (fun () ->
        Mutex.execute m (fun () ->
            created := true;
            Condition.signal c
          );
        f ()
      ) () in
    Mutex.execute m (fun () ->
        while not !created do
          Condition.wait c m
        done);
    thr

  (* pops [n] items in a separate thread from [qs].
     and returns the items in the order they were popped.
     [f] is executed in parallel with the popping thread.
  *)
  let with_pop_thread n qs f =
    let finished = ref false in
    let popped = ref [] in
    let thr = make_thread (fun () ->
        for _ = 1 to n do
          let tag, item = Queues.pop qs in
          popped := (tag, item) :: !popped
        done;
        finished := true;
      ) in
    f ();
    Thread.join thr;
    Alcotest.check Alcotest.bool "pop test finished" true !finished;
    (* When a per-tag queue becomes empty it must not show up anymore *)
    Alcotest.check tags "Per-tag queues are cleaned up" [] (Queues.tags qs);
    List.rev !popped

  let items = Alcotest.(pair string int |> list)

  (* expects only item, pops it and checks it *)
  let with_one_pop_thread (mytag, myitem) qs f =
    let popped = with_pop_thread 1 qs f in
    Alcotest.check Alcotest.bool "queue is empty after pop" true (Queues.get mytag qs |> Queue.is_empty);
    Alcotest.check items "popped items match" [(mytag, myitem)] popped

  (* checks that the pop schedule is round-robin.
     we don't do an exact comparison because even if the point where the RR schedule
     starts is different, or if the tags get picked in a different order it can still
     be a valid schedule if we always pick the oldest queued item from each tag in turn.
  *)
  let check_schedule input schedule =
    Alcotest.check items "all elements got scheduled"
      (List.fast_sort compare input)
      (List.fast_sort compare schedule);

    let schedule_str = let module M = (val items) in Fmt.to_to_string M.pp schedule in
    let schedule_err = "schedule is not RR: " ^ schedule_str in
    List.fold_left (fun (last_i) (_, i) ->
        (* we pick the 1st item from all tags,
           then the 2nd item from all tags, and so on.
           So once we started picking the 3rd item we must not see the
           1st or 2nd items get scheduled.
        *)
        Alcotest.check Alcotest.bool schedule_err true (i >= last_i);
        i
      ) (-1) schedule |> ignore

  let test_ops =
    let mytag = "mytag" in
    let myitem = 42 in
    let with_thread f _ =
      let qs = Queues.create () in
      with_one_pop_thread (mytag, myitem) qs (f qs) in

    (* Test that [n] tags, each with at most [n] items
       get popped in a round-robin fashion *)
    let test_rr n _ =
      let qs =  Queues.create () in
      let tag_of i = "vm" ^ (string_of_int (i+1)) in
      let input =
        Array.init n (fun m ->
            Array.init (n-m) (fun i ->
                tag_of m, (i+1)
              ) |> Array.to_list
          ) |> Array.to_list |> List.flatten in
      if n == 3 then
        let vm1 = "vm1" and vm2 = "vm2" and vm3 = "vm3" in
        Alcotest.check items "vm tags match"
          [vm1, 1; vm1, 2; vm1, 3; vm2, 1; vm2, 2; vm3, 1]
          input;
        List.iter (fun (t,i) ->
            Queues.push_with_coalesce (Some always) t i qs
          ) input;

        let actual_tags = Queues.tags qs |> List.fast_sort compare in
        let expected_tags = Array.init n tag_of
                            |> Array.to_list |> List.fast_sort compare in
        Alcotest.check tags "tags match"
          expected_tags
          actual_tags;

        for i = 0 to n-1 do
          let tag = tag_of i in
          let q = Queues.get tag qs in
          Alcotest.check Alcotest.(list int) "per-tag queue"
            (Array.init (n-i) (fun x -> x+1) |> Array.to_list)
            (Queue.fold (fun acc e -> e :: acc) [] q |> List.rev)
        done;

        let popped = with_pop_thread (List.length input) qs ignore in
        if n == 3 then
          (* pick 1st item from each VM, then pick 2nd item from each, then 3rd *)
          let expected = [vm1, 1; vm2, 1; vm3, 1; vm1, 2; vm2, 2; vm1, 3] in
          Alcotest.check items "per-vm items match" expected popped;
          check_schedule input popped
    in

    [
      "pop, push", `Quick, with_thread (fun qs () ->
          Queues.push_with_coalesce None mytag myitem qs;
        );

      "push, pop", `Quick, (fun _ ->
          let qs = Queues.create () in
          Queues.push_with_coalesce None mytag myitem qs;
          Alcotest.check tags "exactly 1 tag after push" [mytag] (Queues.tags qs);
          with_one_pop_thread (mytag, myitem) qs ignore);

      "push, transfer, pop", `Quick, (fun _ ->
          let qs' = Queues.create () in
          with_thread (fun qs () ->
              Queues.push_with_coalesce None mytag myitem qs';
              Alcotest.check tags "Exactly 1 tag present" [mytag] (Queues.tags qs');

              Queues.transfer_tag mytag qs' qs;
              Alcotest.check tags "Tag removed after transfer" [] (Queues.tags qs');
              (* Do not check destination here because we have an active pop thread:
                 the test wouldn't be deterministic *)
            ) ()
        );

      "RR 3", `Quick, test_rr 3;

      "RR 100", `Quick, test_rr 100
    ]

  let tests : unit Alcotest.test list = [
    "empty queues", [ "empty queues", `Quick, test_empty_queues ];
    "ops", test_ops
  ]
end
let tests = Test.tests
