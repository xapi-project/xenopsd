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
  val dump_item : t -> Rpc.t
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
  type t
  type item
  val create : int -> t
  val set_size : t -> int -> unit
  val push : t -> string -> item -> unit
  val dump : t list -> Rpc.t * Rpc.t
end

module Make(I:Item) = struct
  open I
  type item = I.t
  let describe_item x = x |> I.dump_item |> Jsonrpc.to_string
  module Redirector = struct
    type t = { queues: item Queues.t; mutex: Mutex.t }
    let create () = { queues = Queues.create (); mutex = Mutex.create () }

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

      let make redirectors =
        Mutex.execute m
          (fun () ->
             let queues = List.rev_map (fun t -> t.queues) redirectors in
             let one queue =
               List.map
                 (fun t ->
                    { tag = t; items = List.rev (Queue.fold (fun acc b -> describe_item b :: acc) [] (Queues.get t queue)) }
                 ) (Queues.tags queue) in
             List.concat (List.map one (List.rev_append queues (List.map snd (StringMap.bindings !overrides))))
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
      if t.shutdown_requested && t.state <> Shutdown
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

  type t = Redirector.t

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
  let dump redirectors =
    Redirector.Dump.(make redirectors |> rpc_of_t),
    Dump.(make () |> rpc_of_t)

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

  let set_size queues size =
    let active = count_active queues in
    debug "XXX active = %d" active;
    for i = 1 to max 0 (size - active) do
      incr queues
    done;
    for i = 1 to max 0 (active - size) do
      decr queues
    done

  let create n =
    let t = Redirector.create () in
    if n > 0 then
      set_size t n;
    t

  let push = Redirector.push
end

module Test = struct
  open OUnit2
  let assert_tags ~msg ~expected actual =
    assert_equal ~msg ~printer:(String.concat ",") expected actual

  let assert_string ~expected actual =
    assert_equal ~printer:(fun x -> x) expected actual

  let queues _ = Queues.create ()

  let test_empty_queues _ =
    let qs = Queues.create () in
    let dst = Queues.create () in
    assert_tags ~msg:"Newly created queue is empty" ~expected:[] (Queues.tags qs);
    Queues.transfer_tag "foo" qs dst;
    assert_tags ~msg:"Queue still empty after noop transfer" ~expected:[] (Queues.tags qs);
    assert_tags ~msg:"Destination is empty after noop transfer" ~expected:[] (Queues.tags dst);
    assert_bool "Queue for non-existent tag is empty" (Queues.get "foo" qs |> Queue.is_empty)

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
        for i = 1 to n do
          let tag, item = Queues.pop qs in
          popped := (tag, item) :: !popped
        done;
        finished := true;
      ) in
    f ();
    Thread.join thr;
    assert_bool "pop test finished" !finished;
    (* When a per-tag queue becomes empty it must not show up anymore *)
    assert_tags ~msg:"Per-tag queues are cleaned up" ~expected:[] (Queues.tags qs);
    List.rev !popped

  let items_printer lst =
    List.map (fun (t,i) -> Printf.sprintf "%s,%d" t i) lst |>
    String.concat "; "

  (* expects only item, pops it and checks it *)
  let with_one_pop_thread (mytag, myitem) qs f =
    let popped = with_pop_thread 1 qs f in
    assert_bool "queue is empty after pop" (Queues.get mytag qs |> Queue.is_empty);
    assert_equal ~printer:items_printer [(mytag, myitem)] popped

  (* checks that the pop schedule is round-robin.
     we don't do an exact comparison because even if the point where the RR schedule
     starts is different, or if the tags get picked in a different order it can still
     be a valid schedule if we always pick the oldest queued item from each tag in turn.
  *)
  let check_schedule input schedule =
    assert_equal ~msg:"all elements got scheduled"
      ~printer:items_printer
      (List.fast_sort compare input)
      (List.fast_sort compare schedule);

    let schedule_str = items_printer schedule in
    let schedule_err = "schedule is not RR: " ^ schedule_str in
    List.fold_left (fun (last_i) (t, i) ->
        (* we pick the 1st item from all tags,
           then the 2nd item from all tags, and so on.
           So once we started picking the 3rd item we must not see the
           1st or 2nd items get scheduled.
        *)
        assert_bool schedule_err (i >= last_i);
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
        assert_equal ~printer:items_printer
          [vm1, 1; vm1, 2; vm1, 3; vm2, 1; vm2, 2; vm3, 1]
          input;
        List.iter (fun (t,i) ->
            Queues.push_with_coalesce always t i qs
          ) input;

        let actual_tags = Queues.tags qs |> List.fast_sort compare in
        let expected_tags = Array.init n tag_of
                            |> Array.to_list |> List.fast_sort compare in
        assert_equal ~printer:(String.concat ",")
          ~msg:"tags"
          expected_tags
          actual_tags;

        for i = 0 to n-1 do
          let tag = tag_of i in
          let q = Queues.get tag qs in
          assert_equal
            ~msg:"per-tag queue"
            ~printer:(fun lst ->
                List.map string_of_int lst |> String.concat ",")
            (Array.init (n-i) (fun x -> x+1) |> Array.to_list)
            (Queue.fold (fun acc e -> e :: acc) [] q |> List.rev)
        done;

        let popped = with_pop_thread (List.length input) qs ignore in
        if n == 3 then
          (* pick 1st item from each VM, then pick 2nd item from each, then 3rd *)
          let expected = [vm1, 1; vm2, 1; vm3, 1; vm1, 2; vm2, 2; vm1, 3] in
          assert_equal ~printer:items_printer expected popped;
          check_schedule input popped
    in

    [
      "pop, push" >:: with_thread (fun qs () ->
          Queues.push_with_coalesce always mytag myitem qs;
        );

      "push, pop" >:: (fun _ ->
          let qs = Queues.create () in
          Queues.push_with_coalesce always mytag myitem qs;
          assert_tags ~msg:"exactly 1 tag after push" ~expected:[mytag] (Queues.tags qs);
          with_one_pop_thread (mytag, myitem) qs ignore);

      "push, transfer, pop" >:: (fun _ ->
          let qs' = Queues.create () in
          with_thread (fun qs () ->
              Queues.push_with_coalesce always mytag myitem qs';
              assert_tags ~msg:"Exactly 1 tag present" ~expected:[mytag] (Queues.tags qs');

              Queues.transfer_tag mytag qs' qs;
              assert_tags ~msg:"Tag removed after transfer" ~expected:[] (Queues.tags qs');
              (* Do not check destination here because we have an active pop thread:
                 the test wouldn't be deterministic *)
            ) ()
        );

      "RR 3" >:: test_rr 3;

      "RR 100" >:: test_rr 100
    ]

  let tests = [
    "empty queues" >:: test_empty_queues;
    "queue" >::: test_ops
  ]
end
let tests = Test.tests
