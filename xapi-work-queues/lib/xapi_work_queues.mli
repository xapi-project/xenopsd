(** Work queues
    {e 0.1.0 - {{:https://github.com/xapi-project/xapi-work-queues}homepage}} 

    Consult the {{!S}module documentation}.

    {1 Worker pool scheduling}

    A worker pool has a limited number of worker threads.
    Each worker pops one tagged item from the queue in a round-robin fashion.
    While the item is executed the tag temporarily doesn't participate in round-robin scheduling.
    If during execution more items get queued with the same tag they get redirected to a private queue.
    Once the item finishes execution the tag will participate in RR scheduling again.

    This ensures that items with the same tag do not get executed in parallel,
    and that a tag with a lot of items does not starve the execution of other tags.

    {1 Example usage}

    {[
      module Operation = struct
        type t = string
        let rpc_of_t = Rpc.rpc_of_string
        let execute op = print_endline op
      end

      module WorkerPool = Xapi_work_queues.Make(struct
          type task = Operation.t -> unit
          type t = Operation.t * task

          let describe (op, _) = Rpc.rpc_of_string op
          let diagnostics _ = Rpc.rpc_of_unit ()

          let execute (op, f) = f op
        end)


      let () =
        let pool = WorkerPool.create 25 in
        WorkerPool.push pool "tag1" ("Start" , Operation.execute);
        WorkerPool.push pool "tag2" ("Start", Operation.execute);
        WorkerPool.push pool "tag2" ("Stop", Operation.execute);
        WorkerPool.push pool "tag1" ("Stop", Operation.execute);
        ...
    ]}
*)

(** A work item submitted to a worker pool *)
module type Item =
sig
  (** work item*)
  type t

  (** [describe item] returns a short description of the operation
      for debugging purposes *)
  val describe : t -> Rpc.t

  (** [diagnostics t] dumps information about the task to execute, other than
      the operation defined above *)
  val diagnostics : t -> Rpc.t

  (** [execute item] gets called to run the work item.
      Exceptions raised by [execute] get logged and ignored.
      Calls to [execute] with the same tag are serialised.
  *)
  val execute : t -> unit
end

module type S = sig
  type t (** type of worker pools *)

  type item (** work item *)

  (** [create ?should_keep n] create a worker pool with [n] initial workers.
      If [should_keep current previous] is provided it is used to coalesce work items,
      it determines whether the [current] work item should be retained knowing that the [previous] items in the queue exist.
  *)
  val create : ?should_keep:(item -> item list -> bool) -> int -> t

  (** [set_size pool n] sets the worker [pool] size to [n]. *)
  val set_size : t -> int -> unit

  (** [push pool tag item] pushes [item] at the end of queue for [pool].
      Items with the same [tag] are serialised, but items with different tags
      can be executed in parallel if enough workers are available.
      [tag]s get scheduled in a round-robin fashion.

      You need to start some workers, otherwise none of the items get executed.
  *)
  val push : t -> string -> item -> unit

  (** [diagnostics pool] dumps diagnostic information about the pool *)
  val diagnostics : t list -> Rpc.t * Rpc.t
end

module Make : functor (I : Item) -> S with type item = I.t

val tests : unit Alcotest.test list
