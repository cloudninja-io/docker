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

	if parent == "" {
		if output, err := exec.Command("zfs", "create", "-p", dataset).CombinedOutput(); err != nil {
			return fmt.Errorf("Error ZFS creating dataset: %s (%s)", err, output)
		}
	} else {
		parentDataset := fmt.Sprintf("%s@%s", d.dataset(parent), id)

		if output, err := exec.Command("zfs", "snapshot", parentDataset).CombinedOutput(); err != nil {
			return fmt.Errorf("Error ZFS creating parent snapshot: %s (%s)", err, output)
		}

		if output, err := exec.Command("zfs", "clone", parentDataset, dataset).CombinedOutput(); err != nil {
			return fmt.Errorf("Error ZFS creating dataset: %s (%s)", err, output)
		}

		if output, err := exec.Command("zfs", "destroy", "-d", parentDataset).CombinedOutput(); err != nil {
			return fmt.Errorf("Error ZFS marking dataset: %s (%s)", err, output)
		}
	}

	return nil
}

func (d *Driver) dataset(id string) string {
	return path.Join("storage/docker", path.Base(id))
}

func (d *Driver) Remove(id string) error {
	dataset := d.dataset(id)

	output, err := exec.Command("zfs", "list", "-rt", "snapshot", "-Ho", "name", dataset).Output()
	if err != nil {
		return fmt.Errorf("Error ZFS retrieving children: %s (%s)", err, output)
	}

	snapshots := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")

	for _, snapshot := range snapshots {
		output, err := exec.Command("zfs", "get", "-Ho", "value", "clones", snapshot).Output()
		if err != nil {
			return fmt.Errorf("Error ZFS retrieving clones: %s (%s)", err, output)
		}

		clones := strings.Split(strings.TrimSuffix(string(output), ","), "\n")

		for _, clone := range clones {
			if output, err := exec.Command("zfs", "promote", clone).CombinedOutput(); err != nil {
				return fmt.Errorf("Error ZFS promoting dataset: %s (%s)", err, output)
			}
		}
	}

	if output, err := exec.Command("zfs", "destroy", "-r", dataset).CombinedOutput(); err != nil {
		return fmt.Errorf("Error ZFS destroying dataset: %s (%s)", err, output)
	}

	return nil
}

func (d *Driver) Get(id, mountLabel string) (string, error) {
	dataset := d.dataset(id)

	output, err := exec.Command("zfs", "get", "-Ho", "value", "mountpoint", dataset).Output()
	if err != nil {
		return "", fmt.Errorf("Error ZFS failed to get mountpoint: %s (%s)", err, output)
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
