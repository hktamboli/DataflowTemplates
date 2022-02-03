/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"flag"
	"log"
	"strings"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/erroru"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/flags"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/op"
	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/repo"
)

const (
	CheckstyleCommand = "checkstyle:check"
)

func main() {
	flags.RegisterCommonFlags()
	flag.Parse()

	changed := flags.ChangedFiles()
	if len(changed) == 0 {
		return
	}

	var fullErr error
	for root, children := range repo.GetModulesForPaths(changed) {
		var err error
		if len(children) == 0 {
			err = op.RunMavenOnPom(root, CheckstyleCommand)
		} else {
			err = op.RunMavenOnModule(root, CheckstyleCommand, strings.Join(children, ","))
		}
		fullErr = erroru.CombineErrors(fullErr, err)
	}

	if fullErr != nil {
		log.Fatal("Error running checkstyle. See output for more details.")
	}
}
