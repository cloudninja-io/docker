package zfs

import (
	"fmt"
	"os/exec"
	"path"
	"strings"

	"github.com/docker/docker/daemon/graphdriver"
)

func init() {
	graphdriver.Register("zfs", Init)
}

func Init(home string, options []string) (graphdriver.Driver, error) {
	d := &Driver{
		home: home,
	}
	return d, nil
}

type Driver struct {
	home string
}

func (d *Driver) String() string {
	return "zfs"
}

func (d *Driver) Status() [][2]string {
	return nil
}

func (d *Driver) Cleanup() error {
	return nil
}

func (d *Driver) Create(id, parent string) error {
	dataset := d.dataset(id)

	argv := make([]string, 0, 3)

	if parent == "" {
		argv = append(argv, "create", dataset)
	} else {
		parentDataset := fmt.Sprintf("%s@%s", d.dataset(parent), id)

		if output, err := exec.Command("zfs", "snapshot", parentDataset).CombinedOutput(); err != nil {
			return fmt.Errorf("Error ZFS creating parent snapshot: %s (%s)", err, output)
		}

		argv = append(argv, "clone", fmt.Sprintf("%s@final"), dataset)
	}

	if output, err := exec.Command("zfs", argv...).CombinedOutput(); err != nil {
		return fmt.Errorf("Error ZFS creating dataset: %s (%s)", err, output)
	}

	return nil
}

func (d *Driver) dataset(id string) string {
	return path.Join("storage/docker", path.Base(id))
}

func (d *Driver) Remove(id string) error {
	dataset := d.dataset(id)

	if output, err := exec.Command("zfs", "destroy", dataset).CombinedOutput(); err != nil {
		return fmt.Errorf("Error ZFS destroying dataset: %s (%s)", err, output)
	}

	return nil
}

func (d *Driver) Get(id, mountLabel string) (string, error) {
	dataset := d.dataset(id)

	output, err := exec.Command("zfs", "get", "-Ho", "value", "mountpoint", dataset).Output()
	if err != nil {
		return "", fmt.Errorf("Error ZFS failed to get mountpoint: %s (%s)", dataset, err)
	}

	return strings.TrimSuffix(string(output), "\n"), nil
}

func (d *Driver) Put(id string) {
	// Get() creates no runtime resources (like e.g. mounts)
	// so this doesn't need to do anything.
}

func (d *Driver) Exists(id string) bool {
	_, err := d.Get(id, "")

	return err == nil
}
