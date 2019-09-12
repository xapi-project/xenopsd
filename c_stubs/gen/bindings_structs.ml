open Ctypes
module Make(S: Cstubs_structs.TYPE) = struct
  open S

  module Meminfo = struct
    type meminfo
    type t = meminfo structure
    let t : t typ = structure "xen_sysctl_meminfo"
    let memsize = field t "memsize" uint64_t
    let memfree = field t "memfree" uint64_t
    let () = seal t

    let xen_invalid_mem_sz = constant "XEN_INVALID_MEM_SZ" uint64_t
  end

  module Nodedist = struct
    type t = Unsigned.uint32
    let t = uint32_t
    let xen_invalid_node_dist = constant "XEN_INVALID_NODE_DIST" t
  end

  module Cputopo = struct
    type cputopo
    type t = cputopo structure
    let t : t typ = structure "xen_sysctl_cputopo"
    let core = field t "core" uint32_t
    let socket = field t "socket" uint32_t
    let node = field t "node" uint32_t
    let () = seal t

    let xen_invalid_core_id = constant "XEN_INVALID_CORE_ID" uint32_t
    let xen_invalid_socket_id = constant "XEN_INVALID_SOCKET_ID" uint32_t
    let xen_invalid_node_id = constant "XEN_INVALID_NODE_ID" uint32_t
  end

  module Physdev_pci_device = struct
    type physdev_pci_device
    type t = physdev_pci_device structure
    let t : t typ = structure "physdev_pci_device"
    let seg = field t "seg" uint16_t
    let bus = field t "bus" uint8_t
    let devfn = field t "devfn" uint8_t
    let () = seal t
  end

end
