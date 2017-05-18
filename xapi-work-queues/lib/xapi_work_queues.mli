(** Work queues
    {e 0.1.0 - {{:https://github.com/xapi-project/xapi-work-queues}homepage}} 

    Consult the {{!S}module documentation}.
*)

(** A work item submitted to a worker pool *)
module type Item =
sig
  (** work item*)
  type t

  (** [dump_item item] returns a short description of the operation
      for debugging purposes *)
  val dump_item : t -> Rpc.t

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

module type S = sig
  type t (** type of worker pools *)

  type item (** work item *)

  (** [create n] Create a worker pool with [n] initial workers. *)
  val create : int -> t

  (** [set_size pool n] sets the worker [pool] size to [n]. *)
  val set_size : t -> int -> unit

  (** [push pool tag item] Pushes [item] at the end of queue for [pool].
      Items with the same [tag] are serialised, but items with different tags
      can be executed in parallel if enough workers are available.
      [tag]s get scheduled in a round-robin fashion.

      You need to start some workers, otherwise none of the items get executed.
  *)
  val push : t -> string -> item -> unit

  (** [dump pool] Dumps diagnostic information about the pool *)
  val dump : t list -> Rpc.t * Rpc.t
end

module Make : functor (I : Item) -> S with type item = I.t

val tests :  OUnit2.test list
