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

let test_allocate ~numa ~sockets ~cores ~vms () =
  let h = make_numa ~numa ~sockets ~cores in
  let memsize = Int64.shift_left 1L 34 in
  let mem = Int64.shift_left 1L 30 in
  let memfree = memsize in
  let nodes = Array.init numa (fun node ->
      Planner.NUMANode.v ~node ~memsize ~memfree) in
  Topology.D.debug "Hierarchy: %s" (to_string Hierarchy.typ_of h);
  let vm_cores = max 2 (cores / vms) in
  for i = 1 to vms do
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
      allocate mem
  done

let suite = "topology test",
            ["Allocation of 1 VM on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:1 ~vms:1
            ;"Allocation of 10 VMs on 1 node", `Quick, test_allocate ~numa:1 ~sockets:1 ~cores:8 ~vms:10
            ;"Allocation of 1 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vms:1
            ;"Allocation of 10 VM on 2 nodes", `Quick, test_allocate ~numa:2 ~sockets:2 ~cores:4 ~vms:10
            ;"Allocation of 1 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vms:1
            ;"Allocation of 10 VM on 4 nodes", `Quick, test_allocate ~numa:4 ~sockets:2 ~cores:16 ~vms:10
            ]
