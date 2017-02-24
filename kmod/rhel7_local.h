#if KERNEL_VERSION_NUM == 31000
#if KERNEL_RELEASE_NUM >= 514
#include "514.6.2.el7/nvme.h"
#include "514.6.2.el7/md.h"
#include "514.6.2.el7/raid0.h"
#else	/* KERNEL_RELEASE_NUM */
#error Not a supported kernel release - update the kernel package!
#endif	/* KERNEL_RELEASE_NUM */
#else	/* KERNEL_VERSION_NUM */
#error Unexpected kernel version for RHEL7/CentOS7
#endif	/* KERNEL_VERSION_NUM */
