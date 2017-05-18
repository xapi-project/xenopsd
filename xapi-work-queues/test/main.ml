open OUnit2
open Stdext.Threadext

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

  let dump_item (op, _) = Rpc.rpc_of_string op

  let dump_task (_, (task:handle)) =
    Dump.rpc_of_t {Dump.id = task.id; tag = task.tag}

  let printer s = s

  let execute (op, t) =
    assert_equal ~printer t.id op;
    t.f ()

  let finally (op, t) =
    assert_equal ~printer t.id op;
    t.finally ()

  let should_keep t _ = true
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
  count: int ref;
}

open Lib_worker

let dump ctx = 
  let log rpc =
    logf ctx `Info "%s" (Jsonrpc.to_string rpc)
  in
  WorkerPool.Dump.(make () |> rpc_of_t |> log);
  Lib_worker.Redirector.Dump.(make () |> rpc_of_t |> log)

let test_pool ~workers ~vms ~events ?(errors=0) ctx =
  logf ctx `Info "Setting worker pool size to %d" workers;
  WorkerPool.set_size workers;
  let open Item in

  let running = {
    m = Mutex.create ();
    cond = Condition.create ();
    count = ref 0;
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
          decr running.count;
          Condition.signal running.cond;
          if (i < errors) then
            failwith "testing failures"
        )
    in
    job, { id; tag; f; finally }
  in

  let wait_for_jobs () =
    dump ctx;
    logf ctx `Info "Waiting for all jobs to be processed";
    Mutex.execute running.m (fun () ->
        while !(running.count) > 0 do
          Condition.wait running.cond running.m
        done
      );
    dump ctx
  in

  let shutdown_workers () =
    (* queue shutdowns *)
    WorkerPool.set_size 0;

    dump ctx;

    (* workers need one last job to shut down *)
    if workers == 1 then begin
      let i = 0 in
      let handle = {
        id = "shutdown#" ^ (string_of_int i);
        tag = "";
        f = (fun () -> ());
        finally = (fun () ->
            Mutex.execute running.m (fun () ->
                decr running.count;
                Condition.signal running.cond);
            ());
      } in
      Mutex.execute running.m (fun () -> incr running.count);
      Redirector.push Redirector.default "shutdown" (handle.id, handle);
      Redirector.push Redirector.parallel_queues "shutdown" (handle.id, handle)
    end;
    wait_for_jobs ();

    WorkerPool.set_size (-1);
    dump ctx
  in

  let jobs_and_handles = Array.init events (create_handle "vm") in
  let handles = Array.map snd jobs_and_handles in

  logf ctx `Info "Pushing items onto work queue";
  (* queue but do not execute jobs, have them locked on the mutex *)
  Mutex.execute mutexes.(0) (fun () ->
      Mutex.execute mutexes.(1) (fun () ->
          Array.iteri (fun i h ->
              Mutex.execute running.m (fun () -> incr running.count);
              Redirector.push Redirector.default h.tag (h.id, h)) handles
        );
      WorkerPool.set_size workers;
    );

  wait_for_jobs ();

  shutdown_workers ();
  logf ctx `Info "Checking handles";

  let count_expect msg p =
    let actual = Array.to_list jobs_and_handles |> List.filter p |> List.length in
    let expected = Array.length jobs_and_handles in
    assert_equal ~printer:string_of_int actual expected
  in

  count_expect "should be called" (fun (job, _) -> job.called);
  count_expect "should be finalized" (fun (job, _) -> job.finalized)

let suite =
  "xapi-work-queues" >::: [
    "simple 1" >:: test_pool ~workers:2 ~events:10 ~vms:1;
    "simple 2" >:: test_pool ~workers:2 ~events:10 ~vms:2;
    "basic 1 thread" >:: test_pool ~workers:1 ~events:100 ~vms:100;
    "basic 25 threads" >:: test_pool ~workers:25 ~events:100 ~vms:100;
    "test per tag queueing" >:: test_pool ~workers:1 ~vms:4 ~events:1000;
    "test per tag queueing" >:: test_pool ~workers:25 ~vms:4 ~events:1000;
    "test per tag queueing with errors" >:: test_pool ~workers:25 ~vms:4 ~events:1000 ~errors:14;
  ]

let () =
  (*  Debug.log_to_stdout ();*)
  OUnit2.run_test_tt_main suite
