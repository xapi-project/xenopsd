(** Work queues
    {e 0.1.0 - {{:https://github.com/xapi-project/xapi-work-queues}homepage}} 

    Consult the {{!S}module documentation}.
*)

(** A work item submitted to a worker pool *)
module type Item =
sig
  (** work item*)
  type t

  (** [describe_item item] returns a short description of the operation
      for debugging purposes *)
  val describe_item : t -> string

  (** [dump_task t] dumps information about the task to execute, other than
      the operation defined above *)
  val dump_task : t -> Rpc.t

  (** [execute item] gets called to run the work item.
      Exceptions raised by [execute] get logged and ignored.
      Calls to [execute] with the same tag are serialised.
  *)
  val execute : t -> unit

  (** [finally item] gets called when executing the work item has finished,
      regardless whether it raised an exception or not.
      Exceptions raised by [finally] get logged and ignored.
      Note that calls to [finally] are not serialised!
  *)
  val finally : t -> unit

  (** [should_keep current previous]
      Determines whether the current work item should be retained
      knowing that the [previous] items in the queue exist.
  *)
  val should_keep : t -> t list -> bool
end

module type Dump =
sig
  type t
  val t_of_rpc : Rpc.t -> t
  val rpc_of_t : t -> Rpc.t

  (** Dump the global state of this module *)
  val make : unit -> t
end

module type S = sig
  type item (** work item *)
  module Redirector :
  sig
    (** A redirector queues items, and redirects their execution to a thread
        from the worker pool *)
    type t
    module Dump : Dump

    (** The default queue should be used for all items, except see below *)
    val default : t

    (** We create another queue only for Parallel atoms so as to avoid a situation where
        Parallel atoms can not progress because all the workers available for the
        default queue are used up by other operations depending on further Parallel
        atoms, creating a deadlock.
    *)
    val parallel_queues : t

    (** [push queue tag item] Pushes [item] at the end of [queue].
        Items with the same [tag] are serialised, but items with different tags
        can be executed in parallel if enough workers are available.
        [tag]s get scheduled in a round-robin fashion.

        You need to start some workers, otherwise none of the items get executed.
    *)
    val push : t -> string -> item -> unit
  end
  module WorkerPool :
  sig
    module Dump : Dump
    (** [set_size n] sets the worker pool size to [n]. *)
    val set_size : int -> unit
  end
end

module Make : functor (I : Item) -> S with type item = I.t
