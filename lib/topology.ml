(* similar to Base's validate *)
module type Checked = sig
  type raw
  type t = private raw
  val typ_of: t Rpc.Types.typ

  (**[of_raw raw] constructs a [t] value,
   * raising an exception when an invariant check fails *)
  val of_raw : raw -> t
  val to_raw : t -> raw
end

module Check(E: sig
    type t
    val typ_of: t Rpc.Types.typ
    val invariants: (string * (t -> bool)) list
end) : Checked with type raw := E.t = struct
  type raw = E.t
  type t = E.t [@@deriving rpcty]
  let typ_of = E.typ_of
  let of_raw t =
    let errors = E.invariants |> List.filter (fun (_, f) -> not @@ f t)
                 |> List.map fst in
    match errors with
    | [] -> t
    | errors ->
      let debug = Rpcmarshal.marshal E.typ_of t |> Jsonrpc.to_string in
      invalid_arg (Printf.sprintf "%s: %s" (String.concat "; " errors) debug)
  let to_raw x = x
end

module MemoryNode : sig
  (** A NUMA node *)
  include Checked with type raw := int
  module Distances : sig
    (** A matrix of distances between NUMA nodes.
     * Typical example: [[10, 21] [21, 10]]
     * *)
    include Checked with type raw := int array array


    val dim : t -> int
  end

  (** [distance matrix a b] is the distance between the 2 NUMA nodes.
   * If this is divided by the distance to the node itself you get an approximation
   * of how much slower it is to access remote memory on NUMA node [b] from NUMA node [a]
   * than accessing local memory to NUMA node [a].
   * *)
  val distance : Distances.t -> t -> t -> int
end = struct
  include Check(struct
      type t = int [@@deriving rpcty]
      let invariants =
        [ "NUMA node cannot be negative", fun n -> n >= 0 ]
    end)

  module Distances = struct
    include Check(struct
        type t = int array array [@@deriving rpcty]

        let is_square_matrix matrix =
          let n = Array.length matrix in
          Array.for_all (fun a -> Array.length a = n) matrix

        let invariants =
          [ "Must be a square matrix", is_square_matrix ]
    end)
    let dim (t: t) = Array.length (to_raw t)
  end

  let distance (t: Distances.t) a b = (Distances.to_raw t).(to_raw a).(to_raw b)
end

module Cpu : sig
  module T : sig
    type t = {
      core: int;
      node: MemoryNode.t;
      hier: int list
    }
  end
  include Checked with type raw := T.t
  (** The innermost hierarchical element, currently a CPU core.
   * Abstract, to allow for future topologies that are more complicated
     * *)
  (** [max_cpu] is the maximum number of CPUs supportable by Xen *)
  val max_cpu : int

  (** [sharing a b] is a higher number the more resources the 2 CPUs share *)
  val sharing: t -> t -> int
end = struct
  module T = struct
    type t = {
      core: int;
      node: MemoryNode.t;
      hier: int list;
    } [@@deriving rpcty]
  end
  include T
  let max_cpu = 512
  include Check(struct
      type t = T.t [@@deriving rpcty]
      let invariants =
        [ "core cannot be negative", (fun t -> t.core >= 0)
        ; "core must be in rage", (fun t -> t.core < max_cpu)
        ; "core/socket cannot be negative",
          fun t -> List.for_all (fun i -> i >= 0) t.hier ]
  end)

  let rec sharing level a b = match a, b with
    | e1 :: tl1, e2 :: tl2 ->
      if e1 = e2 then sharing (level+1) tl1 tl2
      else level (* stop when not shared *)
    | [], [] -> level
    | _ :: _, [] | [], _ :: _ -> assert false

  let sharing (a: t) (b: t) =
    let a = to_raw a in let b = to_raw b in
    let numa_shared = if a.T.node = b.T.node then 1 else 0 in
    sharing numa_shared a.hier b.hier
end

module Cpuset : sig
  type t
  val typ_of: t Rpc.Types.typ
  val empty : t
  val v : int list -> t
  val add : int -> t -> t
  val fold: f:(int -> 'a -> 'a) -> init:'a -> t -> 'a
end = struct
  module CS = Set.Make(struct type t = int let compare (a:int) (b:int) = a-b end)
  type cpus = int list [@@deriving rpcty]
  module C = Check(struct
      type t = cpus [@@deriving rpcty]
      let invariants =
          [ "CPU cannot be negative", List.for_all (fun i -> i >= 0)
          ; "CPU must be in rage", List.for_all (fun i -> i < Cpu.max_cpu) ]
  end)
  type t = CS.t
  let empty = CS.empty
  let v l = CS.of_list (C.of_raw l :> int list)
  let add = CS.add
  let fold ~f ~init t = CS.fold f t init
  let typ_of =
    let open Rpc.Types in
    let rpc_of t = Rpcmarshal.marshal typ_of_cpus (CS.elements t) in
    let of_rpc _ = assert false in
    Abstract {
      aname = "cpuset";
      test_data = [];
      rpc_of;
      of_rpc
    }
end

module Hierarchy : sig
  type t

  val v : Cpu.t list -> MemoryNode.Distances.t -> t

  (** [sharing a b] is a higher number the more resources the 2 CPUs share *)
  val sharing: t -> Cpu.t -> Cpu.t -> int
end = struct
  type t = {
    cpus: Cpu.t array;
    distances: MemoryNode.Distances.t;
    node_cpus: Cpuset.t array;
  } [@@deriving rpcty]

  let sharing _ a b = Cpu.sharing a b

  let v cpus distances =
    let numa_nodes = 1 + MemoryNode.Distances.dim distances in
    let max_numa = MemoryNode.of_raw (numa_nodes - 1) in
    let cpus = Array.of_list cpus in
    Array.iteri (fun i e ->
        let e = Cpu.to_raw e in
        if e.Cpu.T.core <> i then
          invalid_arg "Core index mismatch";
        if e.Cpu.T.node > max_numa then
          invalid_arg "Numa node out of range";
      ) cpus;
    let node_cpus = Array.init numa_nodes (fun _ -> Cpuset.empty) in
    Array.iter (fun cpu ->
        let cpu = Cpu.to_raw cpu in
        let node = cpu.Cpu.T.node |> MemoryNode.to_raw in
        node_cpus.(node) <- Cpuset.add cpu.Cpu.T.core node_cpus.(node)
    ) cpus;
    { cpus; distances; node_cpus }
end
