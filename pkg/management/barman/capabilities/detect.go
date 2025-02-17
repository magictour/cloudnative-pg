/*
Copyright The CloudNativePG Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package capabilities

import (
	"fmt"
	"os/exec"
	"regexp"

	"github.com/blang/semver"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/log"
)

// capabilities stores the current Barman capabilities
var capabilities *Capabilities

// Detect barman-cloud executables presence and store the Capabilities
// of the barman-cloud version it finds
func Detect() (*Capabilities, error) {
	version, err := getBarmanCloudVersion(BarmanCloudWalArchive)
	if err != nil {
		return nil, err
	}

	newCapabilities := new(Capabilities)

	if version == nil {
		log.Info("Missing Barman Cloud installation in the operand image")
		return newCapabilities, nil
	}

	newCapabilities.Version = version

	switch {
	case version.GE(semver.Version{Major: 2, Minor: 18}):
		// Tags, added in Barman >= 2.18
		newCapabilities.HasTags = true
		// Barman-cloud-check-wal-archive, added in Barman >= 2.18
		newCapabilities.HasCheckWalArchive = true
		// Snappy compression support, added in Barman >= 2.18
		newCapabilities.HasSnappy = true
		// error codes for wal-restore command added in Barman >= 2.18
		newCapabilities.HasErrorCodesForWALRestore = true
		fallthrough
	case version.GE(semver.Version{Major: 2, Minor: 14}):
		// Retention policy support, added in Barman >= 2.14
		newCapabilities.HasRetentionPolicy = true
		fallthrough
	case version.GE(semver.Version{Major: 2, Minor: 13}):
		// Cloud providers support, added in Barman >= 2.13
		newCapabilities.HasAzure = true
		newCapabilities.HasS3 = true
		fallthrough
	case version.GE(semver.Version{Major: 2, Minor: 19}):
		// Google Cloud Storage support, added in Barman >= 2.19
		newCapabilities.HasGoogle = true
	}

	log.Debug("Detected Barman installation", "newCapabilities", newCapabilities)

	return newCapabilities, nil
}

// barmanCloudVersionRegex is a regular expression to parse the output of
// any barman cloud command when invoked with `--version` option
var barmanCloudVersionRegex = regexp.MustCompile("barman-cloud.* (?P<Version>.*)")

// getBarmanCloudVersion retrieves a barman-cloud subcommand version.
// e.g. barman-cloud-backup, barman-cloud-wal-archive
// It returns nil if the command is not found
func getBarmanCloudVersion(command string) (*semver.Version, error) {
	_, err := exec.LookPath(command)
	if err != nil {
		return nil, nil
	}

	cmd := exec.Command(command, "--version") // #nosec G204
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("while checking %s version: %w", command, err)
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return nil, fmt.Errorf("exit code different from zero checking %s version", command)
	}

	matches := barmanCloudVersionRegex.FindStringSubmatch(string(out))
	version, err := semver.ParseTolerant(matches[1])
	if err != nil {
		return nil, fmt.Errorf("while parsing %s version: %w", command, err)
	}

	return &version, err
}

// CurrentCapabilities retrieves the capabilities of local barman installation,
// retrieving it from the cache if available.
func CurrentCapabilities() (*Capabilities, error) {
	if capabilities == nil {
		var err error
		capabilities, err = Detect()
		if err != nil {
			log.Error(err, "Failed to detect Barman capabilities")
			return nil, err
		}
	}

	return capabilities, nil
}
