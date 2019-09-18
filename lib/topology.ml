module D = Debug.Make(struct let name = "topology" end)
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
          [ "Must be a square matrix", is_square_matrix
          ; "Distances must be non-negative",
            Array.for_all (Array.for_all (fun i -> i > 0))
          ]
    end)
    let dim (t: t) = Array.length (to_raw t)
  end

  let distance (t: Distances.t) a b = (Distances.to_raw t).(to_raw a).(to_raw b)
end

module Cpu : sig
  (** The innermost hierarchical element, currently a CPU core.
   * Abstract, to allow for future topologies that are more complicated
     * *)
  module T : sig
    type t = {
      core: int;
      node: MemoryNode.t;
      hier: int list
    }
  end

  include Checked with type raw := T.t

  val v : core: int -> socket: int -> node: MemoryNode.t -> t

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

  let v ~core ~socket ~node =
    of_raw { core; node; hier = [socket; core] }

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
  val inter: t -> t -> t
  val fold: f:(int -> 'a -> 'a) -> init:'a -> t -> 'a
  val is_empty : t -> bool
  val active: t -> int
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
  let inter = CS.inter
  let is_empty = CS.is_empty

  let active = CS.cardinal

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
  val typ_of: t Rpc.Types.typ

  val v : Cpu.t list -> MemoryNode.Distances.t -> t

  (** [sharing a b] is a higher number the more resources the 2 CPUs share *)
  val sharing: t -> Cpu.t -> Cpu.t -> int

  val to_string : t -> string

  val cpus: t -> MemoryNode.t -> Cpuset.t

  val apply_mask : t -> Cpuset.t -> t
end = struct
  type t = {
    cpus: Cpu.t array;
    nodes: MemoryNode.t list;
    distances: MemoryNode.Distances.t;
    node_cpus: Cpuset.t array;
  } [@@deriving rpcty]

  let sharing _ a b = Cpu.sharing a b
  let to_string t = Rpcmarshal.marshal typ_of t |> Jsonrpc.to_string

  let v cpus distances =
    let numa_nodes = MemoryNode.Distances.dim distances in
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
    let nodes = List.init numa_nodes MemoryNode.of_raw in
    { cpus; distances; nodes; node_cpus }

  let cpus t n = t.node_cpus.(MemoryNode.to_raw n)

  let apply_mask t mask =
    let node_cpus = Array.map (fun cpuset ->
        Cpuset.inter cpuset mask) t.node_cpus in
    { t with node_cpus }
end

module Planner = struct
  module Node = struct
    module T = struct
      type t = {
          node: MemoryNode.t;
          memsize: int64;
          memfree: int64;
        } [@@deriving rpcty]
    end
    include Check(struct
        type t = T.t [@@deriving rpcty]
        let invariants =
          ["memsize must be positive", (fun t -> t.T.memsize >= 0L)
          ;"memfree must be positive", (fun t -> t.T.memfree >= 0L)
          ]
      end)
  end
  module VM = struct
    type t = {
      vcpus: int;
      mem: int64;
      hard_affinity: Cpuset.t
    } [@@deriving rpcty]
  end
  type t = {
    host: Hierarchy.t;
    nodes: Node.t list;
  } [@@deriving rpcty]


  let filter_available t vm =
    { t with nodes = List.filter (fun n ->
          let node = Node.to_raw n in
          node.Node.T.memfree > 0L &&
          not @@ Cpuset.is_empty (Hierarchy.cpus t.host node.Node.T.node)
        ) t.nodes }

  let roundup_div64 a b =
    Int64.div (Int64.add a (Int64.pred b)) b

  let roundup_div a b =
    (a + b - 1) / b

  let plan t vm =
    let t = filter_available { t with host = Hierarchy.apply_mask t.host vm.VM.hard_affinity } vm in
    let max_numa_nodes = Int64.of_int (List.length t.nodes) in
    let plan_on_node node =
      let node = Node.to_raw node in
      let cpus = Hierarchy.cpus t.host node.Node.T.node |> Cpuset.active in
      let splits_mem = max 1L @@ min max_numa_nodes (roundup_div64 vm.VM.mem node.Node.T.memfree) in
      let splits_cpu = max 1 @@ roundup_div vm.VM.vcpus cpus in
      let splits = max (Int64.to_int splits_mem) splits_cpu in
      D.debug "Node %d: %d splits" (MemoryNode.to_raw  node.Node.T.node) splits;
      splits, node
    in
    let pick_smallest_split lst =
      let smallest = lst |> List.map fst |> List.fold_left min max_int in
      List.filter (fun (splits, _) -> splits = smallest) lst
    in
    let node_load_cmp (_, a) (_, b) =
      (* approximation: use free memory *)
      a.Node

    in
    t.nodes |> List.map plan_on_node |> pick_smallest_split |> List.sort node_load_cmp
end
