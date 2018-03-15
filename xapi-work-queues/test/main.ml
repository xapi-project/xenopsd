open Xapi_stdext_threads.Threadext

module Item = struct
  type handle = {
    id: string;
    tag: string;
    f: unit -> unit;
    finally: unit -> unit;
  }
  type t = string * handle

  module Dump = struct
    type t = {
      id: string;
      tag: string;
    } [@@deriving rpc]
  end

  let describe (op, _) = Rpc.rpc_of_string op

  let diagnostics (_, (task:handle)) =
    Dump.rpc_of_t {Dump.id = task.id; tag = task.tag}

  let execute (op, t) =
    Alcotest.check Alcotest.string "id matches" t.id op;
    Xapi_stdext_pervasives.Pervasiveext.finally t.f t.finally
end

module Lib_worker = Xapi_work_queues.Make(Item)

type job = {
  id: string;
  mutable called:  bool;
  mutable finalized: bool;
}

type running = {
  m: Mutex.t;
  cond: Condition.t;
  mutable count: int;
}

open Lib_worker

let dump t =
  let log rpc =
    print_endline (Jsonrpc.to_string rpc)
  in
  let r1, r2 = Lib_worker.diagnostics [t] in
  log r1;
  log r2

let test_pool ?(errors=0) ~workers ~vms ~events () =
  let default = create workers in
  let open Item in

  let running = {
    m = Mutex.create ();
    cond = Condition.create ();
    count = 0;
  } in

  let mutexes = Array.init 2 (fun _ -> Mutex.create ()) in

  let create_handle prefix i =
    let id =  "handle#" ^ (string_of_int i) in
    let tag = prefix ^ "#" ^ (string_of_int (i mod vms)) in
    let job = {
      id; called = false; finalized = false;
    } in
    let f () =
      job.called <- true;
      Mutex.execute mutexes.(i mod 2) (fun () ->
          if (i < errors) then
            failwith "testing failures"
        )
    in
    let finally () =
      job.finalized <- true;
      Mutex.execute running.m (fun () ->
          running.count <- running.count - 1;
          Condition.signal running.cond;
          if (i < errors) then
            failwith "testing failures"
        )
    in
    job, { id; tag; f; finally }
  in

  let wait_for_jobs () =
    dump default;
    print_endline "Waiting for all jobs to be processed";
    Mutex.execute running.m (fun () ->
        while running.count > 0 do
          Printf.printf "Waiting for %d jobs\n" running.count;
          dump default;
          Condition.wait running.cond running.m
        done
      );
    print_endline "All jobs have finished";
    dump default
  in

  let shutdown_workers () =
    (* queue shutdowns *)
    set_size default 0;
    dump default;

    (* workers need one last job to shut down *)
    if workers == 1 then begin
      let i = 0 in
      let handle = {
        id = "shutdown#" ^ (string_of_int i);
        tag = "";
        f = (fun () -> ());
        finally = (fun () ->
            Mutex.execute running.m (fun () ->
                running.count <- running.count - 1;
                Condition.signal running.cond);
            ());
      } in
      Mutex.execute running.m (fun () -> running.count <- running.count + 1);
      push default "shutdown" (handle.id, handle);
    end;
    wait_for_jobs ();

    set_size default (-1);
    dump default
  in

  let jobs_and_handles = Array.init events (create_handle "vm") in
  let handles = Array.map snd jobs_and_handles in

  print_endline "Pushing items onto work queue";
  (* queue but do not execute jobs, have them locked on the mutex *)
  Mutex.execute mutexes.(0) (fun () ->
      Mutex.execute mutexes.(1) (fun () ->
          Array.iteri (fun i h ->
              Mutex.execute running.m (fun () -> running.count <- running.count + 1);
              push default h.tag (h.id, h)) handles
        );
      set_size default workers;
    );

  wait_for_jobs ();

  shutdown_workers ();
  print_endline "Checking handles";

  let count_expect msg p =
    let actual = Array.to_list jobs_and_handles |> List.filter p |> List.length in
    let expected = Array.length jobs_and_handles in
    Alcotest.check Alcotest.int "count" expected actual
  in

  count_expect "should be called" (fun (job, _) -> job.called);
  count_expect "should be finalized" (fun (job, _) -> job.finalized)

let items_printer lst =
  List.map (fun (t,i,_) -> Printf.sprintf "%d,%d" t i) lst |>
  String.concat "; "

module IntMap = Map.Make(struct type t = int let compare = compare end)
module IntSet = Set.Make(struct type t = int let compare = compare end)

let check_schedule ctx schedule n ~workers =
  Alcotest.check Alcotest.int "all events scheduled" n (List.length schedule);
  if workers = 1 then
    (* can only check the exact schedule for 1 worker for now,
       with multiple workers jobs get serialized per-tag, and are not expected to be
       fully RR (they are delayed if another worker is already processing a job for that tag).
       Also they can legitimately get skipped in a RR iteration completely if they just got added back.
    *)
    let schedule_str = items_printer schedule in
    let schedule_err = "schedule is not RR: " ^ schedule_str in
    List.fold_left (fun last_op (vm, op, _) ->
        if op < last_op then
          Alcotest.failf "%s -- [%d] @(%d, %d) scheduled too late, operation %d was already processed for other tags"
            schedule_err (last_op - op) vm op last_op;
        op) (-1) schedule |> ignore;
    (* check non-starvation properties when workers >= 1.
       we must only schedule the same tag if there was no other schedulable tag at the time.
       Operations on same tag must be executed in increasing order of operations.
    *)
    List.fold_left (fun (seen, tags_lastop) (tag, op, schedulable) ->
        begin try
            let lastop = IntMap.find tag tags_lastop in
            if op <= lastop then
              Alcotest.failf "%s -- @(%d, %d) scheduled out of order, last operation on tag %d was %d"
                schedule_err tag op tag lastop;
          with Not_found -> ()
        end;
        (* tags that are schedulable but were not seen in this RR iteration *)
        let unseen_schedulable = IntSet.diff schedulable seen in
        if IntSet.mem tag seen && not (IntSet.is_empty unseen_schedulable) then
          Alcotest.failf "%s -- @(%d, %d) tag scheduled again, but there were other schedulable tags: %s"
                            schedule_err tag op (IntSet.elements unseen_schedulable |> List.rev_map
                                                   string_of_int |> String.concat ",");
        (* when we've seen all the schedulable tags then a new RR iteration starts *)
        (if IntSet.is_empty unseen_schedulable then IntSet.empty else IntSet.add tag seen),
        (IntMap.add tag op tags_lastop)
      ) (IntSet.empty, IntMap.empty) schedule |> ignore



let test_rr ~workers ~events ~vms ~flood ctx =
  let module Item = struct
    type t = string * (unit -> unit) * (unit -> unit)
    let describe (op, _, _) = Rpc.rpc_of_string op
    let diagnostics _ = Rpc.rpc_of_unit ()

    let execute (_, f, _) = f ()
    let finally (_, _, g) = g ()
  end in
  let module XWQ = Xapi_work_queues.Make(Item) in
  let open XWQ in
  Printf.printf "Setting worker pool size to %d\n" workers;

  let default = create 0 in
  Random.init 0x3eed; (* deterministic *)
  let m = Mutex.create () in
  let c = Condition.create () in
  let schedule = ref [] in

  let available_events = ref events in
  let limit = 10 in

  let pending = ref events in

  let vm_active = Array.make events None in

  (* set of tags we know for sure are schedulable.
     There might be more than this due to race condition between a tag being just finished,
     and tracked in this set as schedulable
  *)
  let schedulable = ref IntSet.empty in
  let vm_maxop = Array.make vms 0 in

  let execute vm op () =
    Mutex.execute m (fun () ->
        schedulable := IntSet.remove vm !schedulable;
        schedule := (vm, op, !schedulable) :: !schedule;
        decr pending;
        Condition.signal c;
        begin match vm_active.(vm) with
          | Some other_op ->
            Alcotest.failf "Events for same tag must be serialized, but got conflict on (%d, %d) and (%d, %d)"
                 vm op vm other_op
          | None ->
            vm_active.(vm) <- Some op;
        end;
      );
    Thread.yield ();
    Thread.yield ();
    Mutex.execute m (fun () ->
        vm_active.(vm) <- None;
      )

  in

  let finally vm op () =
    Mutex.execute m (fun () ->
        if op < vm_maxop.(vm) then
          schedulable := IntSet.add vm !schedulable;
      )
  in

  let generate_operations vm =
    let n =
      if vm = vms-1 then !available_events
      else
        (* if testing flood of events assign each VM just 2, and let the last VM generate the rest *)
        let limit = if flood then 2 else limit in
        let lim = min !available_events limit in
        if lim > 0 then Random.int lim else 0
    in
    available_events := !available_events - n;
    vm_maxop.(vm) <- n-1;
    Array.init n (fun op ->
        (Printf.sprintf "vm%d-op%d" vm op), execute vm op, finally vm op)|>
    Array.to_list
  in

  let push_items (tag, lst) =
    List.iter (push default tag) lst
  in

  let vm_tags = Array.init vms (fun vm -> "vm" ^ string_of_int vm) in

  (* we could set the workers here: to execute while pushing,
     although in that case checking the RR property is harder:
     operation numbers will become lower
  *)

  vm_tags |> Array.mapi (fun vm tag -> tag, generate_operations vm) |>
  Array.map (Thread.create push_items) |>
  Array.iter Thread.join;
  set_size default workers;

  Mutex.execute m (fun () ->
      Printf.printf "Waiting for %d events to be processed\n" !pending;
      while !pending > 0 do
        Condition.wait c m
      done
    );
  set_size default 0;
  Printf.printf "Checking schedule for %d\n" events;

  check_schedule ctx (List.rev !schedule) events ~workers


let suite =
  Xapi_work_queues.tests @ [
    "basic", [
      Alcotest.test_case "simple 1" `Quick @@ test_pool ~workers:2 ~events:10 ~vms:1
    ; Alcotest.test_case "simple 2" `Quick@@ test_pool ~workers:2 ~events:10 ~vms:2
    ; Alcotest.test_case "basic 1 thread" `Quick @@ test_pool ~workers:1 ~events:100 ~vms:100
    ; Alcotest.test_case "basic 25 threads" `Quick @@ test_pool ~workers:25 ~events:100 ~vms:100
    ; Alcotest.test_case "test per tag queueing" `Quick @@ test_pool ~workers:1 ~vms:4 ~events:1000
    ; Alcotest.test_case "test per tag queueing" `Quick @@ test_pool ~workers:25 ~vms:4 ~events:1000
    ; Alcotest.test_case "test per tag queueing with errors" `Quick @@ test_pool ~workers:25 ~vms:4 ~events:1000 ~errors:14
    ];
    "test item scheduling is RR",
    List.rev_map (fun workers ->
        List.rev_map (fun vms ->
            List.rev_map (fun events ->
                List.rev_map (fun flood ->
                    Printf.sprintf "%d-%d-%d-%b" workers vms events flood, `Slow, test_rr ~workers ~events ~vms ~flood
                  ) [false; true]
              ) [0; 1; 3; 100; 1000 (*; 10000*)] |> List.flatten
          ) [1; 10; 100; 500] |> List.flatten
      ) [1; 3; 25] |> List.flatten
  ]

let () =
  (*  Debug.log_to_stdout ();*)
  Alcotest.run "xapi-work-queues" suite
