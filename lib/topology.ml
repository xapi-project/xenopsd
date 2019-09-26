module CPUSet = struct
  include Set.Make(struct type t = int let compare x y = Pervasives.compare x y end)
  let pp_dump = Fmt.using to_seq Fmt.(Dump.seq int)
  let all n =
    n |> ArrayLabels.init ~f:(fun x -> x)
    |> ArrayLabels.fold_right ~f:add ~init:empty
end

module NUMATopology : sig
  (** NUMA topology information: distances and CPUs *)
  type t
  type node = private Node of int

  (** [v distances cpu_to_node] stores the topology.
   * [distances] is a square matrix, where .(i).(j) is an approximation
   * to how much slower it is to access memory from node [j] when running on node [i].
   * Distances are normalized to 10, .(i).(i) must equal to 10.
   * Usually distances are symmetric .(i).(j) = .(j).(i), but this is not required.
   * [cpu_to_nodes.(i)] = NUMA node of CPU [i]
   *
   * A typical matrix might look like this:
   *  10 21
   *  21 10
   *
   * A more complicated assymetric distance matrix:
   *  10 16 16 22 16 22 16 22
   *  16 10 22 16 16 22 22 17
   *  16 22 10 16 16 16 16 16
   *  22 16 16 10 16 16 22 22
   *  16 16 16 16 10 16 16 22
   *  22 22 16 16 16 10 22 16
   *  16 22 16 22 16 22 10 16
   *  22 16 16 22 22 16 16 10
   *
  *)
  val v: distances:int array array -> cpu_to_node: int array -> t

  val distance: t -> node -> node -> int

  val cpus: t -> node -> CPUSet.t

  val pp_dump_node: node Fmt.t
  val pp_dump : t Fmt.t
end = struct
  type node = Node of int

  (* no mutation is exposed in the interface,
   * therefore this is immutable *)
  type t = {
    distances: int array array;
    cpu_to_node: node array;
    node_cpus: CPUSet.t array;
  }

  let node_of_int i = Node i

  let v ~distances ~cpu_to_node =
    let node_cpus = Array.map (fun _ -> CPUSet.empty) distances in
    Array.iteri (fun i node ->
        node_cpus.(node) <- CPUSet.add i node_cpus.(node)
    ) cpu_to_node;
    (* TODO: check topology *)
    { distances; cpu_to_node = Array.map node_of_int cpu_to_node; node_cpus }

  let cpus t (Node i) = t.node_cpus.(i)

  let distance t (Node a) (Node b) = t.distances.(a).(b)

  let pp_dump_node = Fmt.(using (fun (Node x) -> x) int)
  let pp_dump =
    Fmt.(Dump.record
           [ Dump.field "distances" (fun t -> t.distances) (Dump.array (Dump.array int))
           ; Dump.field  "cpu2node" (fun t -> t.cpu_to_node) (Dump.array pp_dump_node)
           ; Dump.field "node_cpus" (fun t -> t.node_cpus) (Dump.array CPUSet.pp_dump)
    ])
end

 (* distance: normalized to 10
  * note that ~0U in Xen means no distance,
  * which is -1 for us
  * *)

let check_exn t typ_of properties =
  let errors =
    properties |> List.filter (fun (_, f) -> not @@ f t) |> List.map fst
  in
  match errors with
  | [] ->
      ()
  | errors ->
      let debug = to_string typ_of t in
      invalid_arg (Printf.sprintf "%s: %s" (String.concat ";\n" errors) debug)

(** BoundedInt(T).t will always be in the [gte, lt) range by construction.
 * There are a lot of integer-like types here: a functor is used to create a unique
 * type for each to avoid mixing CPU core number with NUMA index for example.
 * *)
module BoundedInt (E : sig
  val gte : int

  val lt : int
end) : sig
  type t = private int
  (** We allow the compiler to see it is an int, but the only way to
   * construct one is via the provided [v] function *)

  val typ_of : t Rpc.Types.typ

  val v : int -> t
  (** [v i] is equivalent to [i].
   * Raises an exception if outside [gte, lt) range. *)

  val to_int : t -> int

  val compare : t -> t -> int
end = struct
  type t = int [@@deriving rpcty]

  let v n =
    let ok = n >= E.gte && n < E.lt in
    if not ok then
      invalid_arg
      @@ Printf.sprintf "expected range: %d <= %d < %d" E.gte n E.lt
    else n

  let compare a b = a - b

  let to_int x = x
end

(* helper for writing typ_of that cannot be automatically derived *)
let typ_of_using aname typ_of ~f ~inv =
  let open Rpc.Types in
  let rpc_of d = Rpcmarshal.marshal typ_of @@ f d in
  let of_rpc r =
    match Rpcmarshal.unmarshal typ_of r with
    | Ok r ->
        Ok (inv r)
    | Error e ->
        Error e
  in
  Abstract {aname; test_data= []; rpc_of; of_rpc}

(* All data structures used here are immutable,
 * however an array is mutable.
 * Hide the array, and only expose read-only accessors.
 * This makes it safe to share the array between multiple copies, knowing it won't change *)
module Indexed (E : sig
    de
  type t

  val typ_of : t Rpc.Types.typ
end) : sig
  type t

  val typ_of : t Rpc.Types.typ

  val v : E.t array -> t

  val get : t -> int -> E.t

  val iter : (E.t -> unit) -> t -> unit
  val iteri : (int -> E.t -> unit) -> t -> unit

  val fold_left : ('a -> E.t -> 'a) -> 'a -> t -> 'a

  val length : t -> int

  val map : (E.t -> E.t) -> t -> t
end = struct
  type t = E.t array [@@deriving rpcty]

  let v a = a

  let get a i = a.(i)

  let iter = Array.iter

  let iteri = Array.iteri

  let fold_left = Array.fold_left

  let length = Array.length

  let map = Array.map
end

(* Xen's maximum *)
let max_cpu = 512

let max_nodes = max_cpu

(** A NUMA node.
 * Accessing memory on a local NUMA node
 * is signficantly faster than on a remote NUMA node
 * *)
module Node = BoundedInt (struct
  let gte = 0

  let lt = max_nodes
end)

(** A CPU logical core, the smallest unit of scheduling granularity *)
module CPU = BoundedInt (struct
  let gte = 0

  let lt = max_cpu
end)

module CPUHier = BoundedInt (struct
  let gte = 0

  let lt = max_cpu
end)

(** Topology showing the physical hierarchy of a core,
 * and the memory hierarchy (NUMA node) *)
module CPUTopo = struct
  type t = {core: CPUHier.t; node: Node.t; hier: CPUHier.t list} [@@deriving rpcty]

  let v ~core ~socket ~node =
    let core = CPUHier.v core in
    let socket = CPUHier.v socket in
    let node = Node.v node in
    {core; node; hier= [socket; core]}

  let rec sharing level a b =
    match (a, b) with
    | e1 :: tl1, e2 :: tl2 ->
        if e1 = e2 then sharing (level + 1) tl1 tl2
        else level (* stop when not shared *)
    | [], [] ->
        level
    | _ :: _, [] | [], _ :: _ ->
        assert false

  (** [sharing a b] returns the number of HW resources shared between the cores. *)
  let sharing a b =
    let numa_shared = if a.node = b.node then 1 else 0 in
    sharing numa_shared a.hier b.hier
end

module Distances : sig
  (** Distances between NUMA nodes *)
  type t

  val v: int array array -> t

  val typ_of : t Rpc.Types.typ

  (** [nodes t] is the number of NUMA nodes *)
  val nodes : t -> int

  (** [distance t a b] is the distance between node [a] and [b] *)
  val distance : t -> Node.t -> Node.t -> int
end = struct
  type t = int array array [@@deriving rpcty]

  let nodes = Array.length

  let v t =
    let n = Array.length t in
    assert (Array.for_all (fun a -> Array.length a = n) t) ;
    t

  let distance t a b = t.(Node.to_int a).(Node.to_int b)
end

(* Quick access to topology information about a core *)
module CPUIndex = struct
  include Indexed (CPUTopo)

  let get t c = get t (CPU.to_int c)
end

(** CPU set or CPU mask: used for hard and soft affinity. *)
module CPUSet = struct
  include Set.Make (CPU)

  type cpus = CPU.t list [@@deriving rpcty]

  let typ_of =
    let open Rpc.Types in
    let rpc_of t = Rpcmarshal.marshal typ_of_cpus (elements t) in
    let of_rpc _ = assert false in
    Abstract {aname= "cpuset"; test_data= []; rpc_of; of_rpc}

  let all n =
    let a = Array.init n CPU.v in
    Array.fold_right add a empty
end

module Hierarchy : sig
  type t
  val typ_of: t Rpc.Types.typ

  val v: CPUIndex.t -> Distances.t -> t

  val sharing : t -> CPU.t -> CPU.t -> int

  val nodes: t -> int

  val to_string : t -> string

  val cpuset_of_node : t -> Node.t -> CPUSet.t

  val node_of_cpu: t -> CPU.t -> Node.t

  val distance: t -> CPU.t -> Node.t -> int

  val distance_node: t -> Node.t -> Node.t -> int

  val all: t -> CPUSet.t
  val apply_mask : t -> CPUSet.t -> t
end = struct
  module Node2CPU = Indexed (CPUSet)

  type t = {cpus: CPUIndex.t; distances: Distances.t; node_cpus: Node2CPU.t}
  [@@deriving rpcty]

  let nodes t = Node2CPU.length t.node_cpus

  let all t =
    CPUSet.all (CPUIndex.length t.cpus)

  let node_of_cpu t cpu = (CPUIndex.get t.cpus cpu).CPUTopo.node
  let distance t cpu node2 =
    Distances.distance t.distances (node_of_cpu t cpu) node2

  let distance_node t = Distances.distance t.distances

  let invariant t =
    let max_used_node =
      CPUIndex.fold_left
        (fun a c -> max a @@ Node.to_int c.CPUTopo.node)
        0 t.cpus
    in
    let max_node = max_used_node + 1 in
    let distances_ok t = Distances.nodes t.distances >= max_node in
    let nodes_ok t = Node2CPU.length t.node_cpus = max_node in
    check_exn t typ_of
      [ ("Distances available for all NUMA nodes", distances_ok)
      ; (Printf.sprintf "All NUMA nodes have a CPUSet: %d = %d"
           (Node2CPU.length t.node_cpus)
           max_node, nodes_ok) ]

  let v cpus distances =
    let nodes = Distances.nodes distances in
    let node_cpus = Array.init nodes (fun _ -> CPUSet.empty) in
    CPUIndex.iteri
      (fun i c ->
        let n = Node.to_int c.CPUTopo.node in
        node_cpus.(n) <- CPUSet.add (CPU.v i) node_cpus.(n))
      cpus ;
    let t = {cpus; distances; node_cpus= Node2CPU.v node_cpus} in
    invariant t ; t

  let sharing t a b =
    let cpu i = CPUIndex.get t.cpus i in
    CPUTopo.sharing (cpu a) (cpu b)

  let to_string = to_string typ_of

  let cpuset_of_node t n = Node2CPU.get t.node_cpus (Node.to_int n)

  let apply_mask t mask =
    let node_cpus = Node2CPU.map (CPUSet.inter mask) t.node_cpus in
    {t with node_cpus}
end

module Planner = struct
  module NUMANode = struct
    type t = {
      node: Node.t;
      memsize: int64;
      memfree: int64;
    } [@@deriving rpcty]

    let v ~node ~memsize ~memfree =
      { node = Node.v node; memsize; memfree }
  end
  module VM = struct
    type t = {
      vcpus: CPU.t;
      mem: int64;
      affinity: CPUSet.t;
    } [@@deriving rpcty]

    let empty = {
      vcpus = CPU.v 0;
      mem = 0L;
      affinity = CPUSet.empty
    }

    let fits vm ~into =
      Int64.compare vm.mem into.mem <= 0 &&
      CPUSet.(inter vm.affinity into.affinity |> cardinal >= CPU.to_int vm.vcpus)

    let union vm1 vm2 =
      let affinity = CPUSet.union vm1.affinity vm2.affinity in
      { vcpus = CPUSet.cardinal affinity |> CPU.v;
        mem = Int64.add vm1.mem vm2.mem;
        affinity
      }
  end

  type t = {
    host: Hierarchy.t;
    nodes: NUMANode.t list;
  } [@@deriving rpcty]

  let v host nodes = { host; nodes }

  let vm_allocation_of_node t node =
    let affinity = Hierarchy.cpuset_of_node t.host node.NUMANode.node in
    { VM.vcpus = CPUSet.cardinal affinity |> CPU.v; mem = node.NUMANode.memfree; affinity }

  let filter_available t vm =
    { t with nodes = List.filter (fun n ->
          n.NUMANode.memfree > 0L &&
          not @@ CPUSet.is_empty (Hierarchy.cpuset_of_node t.host n.NUMANode.node)
        ) t.nodes }

  let roundup_div64 a b =
    Int64.div (Int64.add a (Int64.pred b)) b

  let roundup_div a b =
    (a + b - 1) / b

  let plan t vm =
    let t = filter_available { t with host = Hierarchy.apply_mask t.host vm.VM.affinity } vm in
    let max_numa_nodes = Int64.of_int (List.length t.nodes) in
    let plan_on_node node =
      let cpus = Hierarchy.cpuset_of_node t.host node.NUMANode.node |> CPUSet.cardinal in
      let splits_mem = max 1L @@ min max_numa_nodes (roundup_div64 vm.VM.mem node.NUMANode.memfree) in
      let splits_cpu = max 1 @@ roundup_div (CPU.to_int vm.VM.vcpus) cpus in
      let splits = max (Int64.to_int splits_mem) splits_cpu in
      D.debug "Node %d: %d (%Ld; %d) splits" (Node.to_int node.NUMANode.node)
        splits splits_mem splits_cpu;
      splits, node
    in
    (* 1 split is always better, but when we have multiple splits
     * we need to look at smallest maximum distance,
     * and smallest average, so let the next sorting step pick *)
    let pick_best_split lst =
      match List.filter (fun (splits, _) -> splits = 1) lst with
      | [] -> lst
      | r -> r
    in
    let node_load_cmp (_, a) (_, b) =
      (* approximation: use free memory *)
      Int64.compare b.NUMANode.memfree a.NUMANode.memfree
    in
    let pick_nodes (allocated_vm, allocated_nodes) (_, candidate) =
      if VM.fits vm ~into:allocated_vm then (allocated_vm, allocated_nodes)
      else VM.union allocated_vm (vm_allocation_of_node t candidate),
           candidate :: allocated_nodes
    in
    let node_distance_cmp r (_, a) (_, b) =
      (Hierarchy.distance_node t.host r.NUMANode.node a.NUMANode.node) -
      (Hierarchy.distance_node t.host r.NUMANode.node b.NUMANode.node)
    in
    let allocated_vm, allocated_nodes =
      let nodes = t.nodes |> List.map plan_on_node |> pick_best_split |> List.sort node_load_cmp in
      let _, first = List.hd nodes in
      let rest =  nodes |> List.tl |> List.sort (node_distance_cmp first) in
      List.fold_left pick_nodes (vm_allocation_of_node t first, [first]) rest
    in
    D.debug "Allocated VM: %s, Required VM: %s"
      (to_string VM.typ_of allocated_vm) (to_string VM.typ_of vm);
    D.debug "Picked NUMA nodes: %s" (List.map (to_string NUMANode.typ_of) allocated_nodes |>
                                     String.concat "; ");
    if VM.fits vm ~into:allocated_vm then
      Some (allocated_nodes, CPUSet.inter allocated_vm.VM.affinity vm.VM.affinity)
    else None
end
