package adminui

import (
	"embed"
	"html/template"
)

//go:embed templates/dashboard.html
var adminUITemplateFS embed.FS

func dashboardTemplate() (*template.Template, error) {
	return template.ParseFS(adminUITemplateFS, "templates/dashboard.html")
}
