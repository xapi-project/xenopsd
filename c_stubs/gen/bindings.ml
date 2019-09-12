open Ctypes
module Types = Bindings_structs_lib.Bindings_structs.Make(Generated_types)
module Make(F: Cstubs.FOREIGN) = struct
  open F

  type handle = unit ptr
  let handle : handle typ = ptr void

  let interface_open = foreign "xc_interface_open" (ptr_opt void @-> ptr_opt void @-> uint @-> returning handle)
  let interface_close = foreign "xc_interface_close" (handle @-> returning int)

  let numainfo = foreign "xc_numainfo" (handle @-> ptr uint @-> ptr_opt Types.Meminfo.t @-> ptr_opt Types.Nodedist.t @-> returning int)
  let cputopoinfo = foreign "xc_cputopoinfo" (handle @-> ptr uint @-> ptr_opt Types.Cputopo.t @-> returning int)
  let pcitopoinfo = foreign "xc_pcitopoinfo" (handle @-> uint @-> ptr_opt Types.Physdev_pci_device.t @-> ptr_opt uint32_t @-> returning int)
end
