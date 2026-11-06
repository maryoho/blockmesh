obj-m += dm-blockmesh.o
dm-blockmesh-y := dm-blockmesh-target.o bm_chrdev.o
KDIR ?= /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules
clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
