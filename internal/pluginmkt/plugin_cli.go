package pluginmkt

import (
	"context"
	"fmt"
	"strings"
)

// Search searches for plugins
func (cli *PluginCLI) Search(query string, pluginType string) error {
	opts := SearchOptions{
		Query:    query,
		SortBy:   "downloads",
		SortDesc: true,
		Limit:    20,
	}
	if pluginType != "" {
		opts.Type = PluginType(pluginType)
	}

	results, err := cli.registry.Search(context.Background(), opts)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Fprintln(cli.output, "No plugins found")
		return nil
	}

	fmt.Fprintf(cli.output, "Found %d plugins:\n\n", len(results))
	for _, p := range results {
		verified := ""
		if p.Verified {
			verified = " ✓"
		}
		fmt.Fprintf(cli.output, "  %s%s (%s)\n", p.Name, verified, p.Version)
		fmt.Fprintf(cli.output, "    %s\n", p.Description)
		fmt.Fprintf(cli.output, "    Downloads: %d | Rating: %.1f\n\n", p.Downloads, p.Rating)
	}

	return nil
}

// Install installs a plugin
func (cli *PluginCLI) Install(pluginID string, force bool) error {
	fmt.Fprintf(cli.output, "Installing %s...\n", pluginID)

	opts := InstallOptions{Force: force}
	if err := cli.registry.Install(context.Background(), pluginID, opts); err != nil {
		return fmt.Errorf("installation failed: %w", err)
	}

	fmt.Fprintf(cli.output, "✓ Successfully installed %s\n", pluginID)
	return nil
}

// Uninstall removes a plugin
func (cli *PluginCLI) Uninstall(pluginID string) error {
	fmt.Fprintf(cli.output, "Uninstalling %s...\n", pluginID)

	if err := cli.registry.Uninstall(context.Background(), pluginID); err != nil {
		return fmt.Errorf("uninstall failed: %w", err)
	}

	fmt.Fprintf(cli.output, "✓ Successfully uninstalled %s\n", pluginID)
	return nil
}

// List lists installed plugins
func (cli *PluginCLI) List() error {
	installed := cli.registry.ListInstalled()

	if len(installed) == 0 {
		fmt.Fprintln(cli.output, "No plugins installed")
		return nil
	}

	fmt.Fprintf(cli.output, "Installed plugins (%d):\n\n", len(installed))
	for _, p := range installed {
		state := string(p.State)
		fmt.Fprintf(cli.output, "  %s (%s) [%s]\n", p.Metadata.Name, p.Metadata.Version, state)
		fmt.Fprintf(cli.output, "    Type: %s | Installed: %s\n\n",
			p.Metadata.Type, p.InstalledAt.Format("2006-01-02"))
	}

	return nil
}

// Update updates plugins
func (cli *PluginCLI) Update(pluginID string) error {
	if pluginID == "" {

		updates, err := cli.registry.CheckUpdates(context.Background())
		if err != nil {
			return err
		}

		if len(updates) == 0 {
			fmt.Fprintln(cli.output, "All plugins are up to date")
			return nil
		}

		for _, u := range updates {
			fmt.Fprintf(cli.output, "Updating %s (%s -> %s)...\n",
				u.PluginID, u.CurrentVersion, u.LatestVersion)
			if err := cli.registry.Update(context.Background(), u.PluginID); err != nil {
				fmt.Fprintf(cli.output, "  ✗ Failed: %v\n", err)
			} else {
				fmt.Fprintf(cli.output, "  ✓ Updated\n")
			}
		}
	} else {
		fmt.Fprintf(cli.output, "Updating %s...\n", pluginID)
		if err := cli.registry.Update(context.Background(), pluginID); err != nil {
			return fmt.Errorf("update failed: %w", err)
		}
		fmt.Fprintf(cli.output, "✓ Successfully updated %s\n", pluginID)
	}

	return nil
}

// Info shows plugin details
func (cli *PluginCLI) Info(pluginID string) error {
	meta, err := cli.registry.GetPlugin(context.Background(), pluginID)
	if err != nil {
		return err
	}

	fmt.Fprintf(cli.output, "Plugin: %s\n", meta.Name)
	fmt.Fprintf(cli.output, "ID: %s\n", meta.ID)
	fmt.Fprintf(cli.output, "Version: %s\n", meta.Version)
	fmt.Fprintf(cli.output, "Type: %s\n", meta.Type)
	fmt.Fprintf(cli.output, "Author: %s\n", meta.Author)
	fmt.Fprintf(cli.output, "License: %s\n", meta.License)
	fmt.Fprintf(cli.output, "Description: %s\n", meta.Description)
	fmt.Fprintf(cli.output, "\n")

	if meta.Verified {
		fmt.Fprintf(cli.output, "✓ Verified\n")
	}

	fmt.Fprintf(cli.output, "Downloads: %d\n", meta.Downloads)
	fmt.Fprintf(cli.output, "Rating: %.1f (%d ratings)\n", meta.Rating, meta.RatingCount)

	if len(meta.Capabilities) > 0 {
		fmt.Fprintf(cli.output, "\nCapabilities:\n")
		for _, cap := range meta.Capabilities {
			fmt.Fprintf(cli.output, "  - %s\n", cap)
		}
	}

	if len(meta.Dependencies) > 0 {
		fmt.Fprintf(cli.output, "\nDependencies:\n")
		for _, dep := range meta.Dependencies {
			fmt.Fprintf(cli.output, "  - %s", dep.ID)
			if dep.MinVersion != "" {
				fmt.Fprintf(cli.output, " (>= %s)", dep.MinVersion)
			}
			fmt.Fprintln(cli.output)
		}
	}

	if len(meta.Platforms) > 0 {
		fmt.Fprintf(cli.output, "\nPlatforms: %s\n", strings.Join(meta.Platforms, ", "))
	}

	return nil
}
