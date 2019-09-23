open Topology

let make_numa ~numa ~sockets ~cores =
  let distances = Array.init numa (fun i ->
      Array.init numa (fun j ->
          if i = j then 10
          else 10 + 11 * abs (j - i)
  )) |> Distances.v in
  let cores_per_numa = cores / numa in
  let cores_per_socket = cores / sockets in
  let cpus = Array.init cores (fun core ->
    let node = core / cores_per_numa in
    let socket = core / cores_per_socket in
    CPUTopo.v ~core ~socket ~node
  ) |> CPUIndex.v in
  Hierarchy.v cpus distances

type t = {
  worst: int;
  average: float;
  bandwidth: float;
  best: int;
} [@@deriving rpcty]

let sum_costs l =
  D.debug "====";
  List.fold_left (fun accum cost ->
      D.debug "bandwidth += %f" cost.bandwidth;
      { worst = max accum.worst cost.worst
      ; average = accum.average +. cost.average
      ; bandwidth = accum.bandwidth +. cost.bandwidth
      ; best = min accum.best cost.best }
    ) { worst = min_int; average = 0.; bandwidth = 0.; best = max_int } l

let vm_access_costs host all_vms (vcpus, nodes, cpuset) =
  let all_vms = ((vcpus, nodes), cpuset) :: all_vms in
  let nodes = List.map (fun n -> n.Planner.NUMANode.node) nodes in
  let n = List.length nodes in
  let slice_of vcpus cpuset =
    float vcpus /. float (CPUSet.cardinal cpuset) in
  (* a simple model with a single interconnect,
   * assuming all non-local accesses go through it *)
  let interconnect_slice_of ((_, nodes), cpuset) =
    (* percentage of time that an access is a remote access *)
    let n = List.length nodes in
    let remote_percentage = float (n - 1) /. float n in
    remote_percentage *. (float (CPUSet.cardinal cpuset))
  in
  let all_interconnect_slices =
    all_vms |> List.map interconnect_slice_of
    |> List.fold_left (+.) 0.
  in
  let want_slice = slice_of vcpus cpuset in
  let used_cpus =
    all_vms |> List.map snd |>
    List.fold_left CPUSet.union CPUSet.empty in
  let costs = cpuset |> CPUSet.elements |> List.map (fun c ->
      let distances = List.map (Hierarchy.distance host c) nodes in
      let worst = List.fold_left max 0 distances in
      let best = List.fold_left min max_int distances in
      let average = float (List.fold_left (+) 0 distances) /. float n in
      (* if all VMs run their CPUs 100% in the recommended soft affinity
       * how many time slices can this vCPU get?
       * *)
      D.debug "--";
      let all_slices =
        List.fold_left (fun acc ((vcpus, _), cpuset) ->
            D.debug "CPU %d, CPUSet: %s; slice: %f" (CPU.to_int c)
              (to_string CPUSet.typ_of cpuset) (slice_of vcpus cpuset);
            if CPUSet.mem c cpuset then
              acc +. slice_of vcpus cpuset
            else acc
        ) 0. all_vms in
      let cpu_slice = want_slice /. all_slices in
      D.debug "cpu_slice: %f out of %f" want_slice all_slices;
      assert (want_slice <= all_slices);
      (* if all CPUs in this NUMA node are busy accessing local memory,
       * how much of that bandwidth can this cpu get? *)
      let numa_local_slice =
        1. /. (float
        (CPUSet.inter
          (Hierarchy.cpuset_of_node host (Hierarchy.node_of_cpu host c))
          used_cpus
        |> CPUSet.cardinal))
      in
      (* we got N nodes, the local node is accessed only 1/N times *)
      let numa_local_slice = numa_local_slice /. (float (List.length nodes)) in
      D.debug "NUMA local slice: %f" numa_local_slice;
      let numa_remote_slice =
        let my_slice = interconnect_slice_of (((), nodes), CPUSet.singleton c) in
        if my_slice <> 0. then
          my_slice /. all_interconnect_slices
        else 0.
      in
      D.debug "NUMA remote slice: %f" numa_remote_slice;
      (* assume interconnect has half bandwidth of local node *)
      let bandwidth = cpu_slice *. (numa_local_slice +. numa_remote_slice /. 2.) in
      D.debug "bandwidth: %f" bandwidth;
      { worst; best; bandwidth; average }
    ) |> sum_costs in
  let cpus = float @@ CPUSet.cardinal cpuset in
  { costs with average = costs.average /. cpus;
               bandwidth = costs.bandwidth *. slice_of vcpus cpuset
  }

let cost_not_worse ~default c =
  let worst = max default.worst c.worst in
  let best = min default.best c.best in
  let average = min default.average c.average in
  D.debug "Default access times: %s; New plan: %s" (to_string typ_of default) (to_string typ_of c);
  Alcotest.(check int "The worst-case access time should not be changed from default" default.worst worst);
  Alcotest.(check int "Best case access time should not change" best c.best);
  Alcotest.(check (float 1e-3) "Average access times could improve" average c.average);
  if c.best < default.best then
    D.debug "The new plan has improved the best-case access time!";
  if c.worst < default.worst then
    D.debug "The new plan has improved the worst-case access time!";
  if c.average < default.average then
    D.debug "The new plan has improved the average access time!"

let check_aggregate_costs_not_worse (default, next, _) =
  let default = sum_costs default in
  let next = sum_costs next in
  cost_not_worse ~default next;
  let bandwidth = max default.bandwidth next.bandwidth in
  Alcotest.(check (float 1e-3) "Bandwidth could improve" bandwidth next.bandwidth);
  if next.bandwidth > default.bandwidth then
    D.debug "Bandwidth has improved!"


let test_allocate ~numa ~sockets ~cores ~vms () =
  let h = make_numa ~numa ~sockets ~cores in
  let memsize = Int64.shift_left 1L 34 in
  let mem = Int64.shift_left 1L 30 in
  let memfree = memsize in
  let nodes = Array.init numa (fun node ->
      Planner.NUMANode.v ~node ~memsize ~memfree) in
  Topology.D.debug "Hierarchy: %s" (to_string Hierarchy.typ_of h);
  let vm_cores = max 2 (cores / vms) in
  List.init vms (fun i -> i+1)
  |> List.fold_left (fun (costs_old, costs_new, plans) i ->
    Topology.D.debug "Planning VM %d" i;
    let p = Planner.v h (Array.to_list nodes) in
    let hard_affinity = CPUSet.all cores in
    let vm = Planner.VM.{ vcpus = CPU.v vm_cores; mem; hard_affinity } in
    match Planner.plan p vm with
    | None -> Alcotest.fail "No NUMA plan"
    | Some (usednodes, plan) ->
      Topology.D.debug "NUMA allocation succeeded for VM %d: %s"
        i (Topology.to_string CPUSet.typ_of plan);
      let rec allocate mem =
        let mem_try = Int64.div mem (List.length usednodes |> Int64.of_int) in
        if mem_try > 0L then
          let mem_allocated = List.fold_left (fun mem n ->
              let idx = Node.to_int (n.Planner.NUMANode.node) in
              let memfree = max 0L (Int64.sub nodes.(idx).Planner.NUMANode.memfree mem_try) in
              let delta = Int64.sub nodes.(idx).memfree memfree in
              nodes.(idx) <- { nodes.(idx) with memfree };
              Int64.add mem delta
            ) 0L usednodes in
          allocate @@ Int64.sub mem mem_allocated
      in
      allocate mem;
      let costs_numa_aware = vm_access_costs h plans (vm_cores, usednodes, plan) in
      let costs_default = vm_access_costs h plans (vm_cores, Array.to_list nodes, Hierarchy.all h) in
      cost_not_worse ~default:costs_default costs_numa_aware;
      costs_default :: costs_old, costs_numa_aware :: costs_new, ((vm_cores, usednodes), plan) :: plans
    ) ([], [], []) |> check_aggregate_costs_not_worse

let suite = "topology test",
            ["Allocation of 1 VM on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:2 ~vms:1
            ;"Allocation of 10 VMs on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:8 ~vms:10
            ;"Allocation of 1 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vms:1
            ;"Allocation of 10 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vms:10
            ;"Allocation of 1 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vms:1
            ;"Allocation of 10 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vms:10
            ]
