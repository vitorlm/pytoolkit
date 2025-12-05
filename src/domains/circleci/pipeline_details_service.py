"""CircleCI Pipeline Details Service
Service for extracting detailed information from a specific CircleCI pipeline
"""

import time
from datetime import datetime
from typing import Any

import requests

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager


class PipelineDetailsService:
    """Service for extracting detailed information from a specific CircleCI pipeline"""

    def __init__(self, token: str, project_slug: str):
        """Initialize CircleCI Pipeline Details service

        Args:
            token: CircleCI API token
            project_slug: Project slug (e.g., 'gh/organization/repository-name')
        """
        self.token = token
        self.project_slug = project_slug
        self.base_url = "https://circleci.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update({"Circle-Token": token, "Accept": "application/json"})
        self.logger = LogManager.get_instance().get_logger("PipelineDetailsService")
        self.cache = CacheManager.get_instance()

    def _make_request(self, endpoint: str, params: dict | None = None) -> dict:
        """Make API request to CircleCI with error handling"""
        url = f"{self.base_url}{endpoint}"

        try:
            self.logger.debug(f"Making request to: {url}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to make request to {url}: {e}")
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response body: {e.response.text}")
            raise

    def get_pipeline_by_number(self, pipeline_number: int) -> dict | None:
        """Get pipeline by number using the project pipelines endpoint"""
        self.logger.info(f"🔍 Searching for pipeline number {pipeline_number}")

        try:
            # Get recent pipelines for the project
            endpoint = f"/project/{self.project_slug}/pipeline"
            params = {"page-token": None}

            # Search through multiple pages if needed
            for page in range(5):  # Limit to 5 pages to avoid infinite loop
                data = self._make_request(endpoint, params)

                for pipeline in data.get("items", []):
                    if pipeline.get("number") == pipeline_number:
                        self.logger.info(f"✅ Found pipeline {pipeline_number}: {pipeline.get('id')}")
                        return pipeline

                # Check for next page
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break
                params["page-token"] = next_page_token

            self.logger.warning(f"⚠️  Pipeline number {pipeline_number} not found in recent pipelines")
            return None

        except Exception as e:
            self.logger.error(f"❌ Error searching for pipeline {pipeline_number}: {e}")
            return None

    def get_pipeline_details(
        self,
        pipeline_id: str | None = None,
        pipeline_number: int | None = None,
        verbose: bool = False,
    ) -> dict | None:
        """Get comprehensive details for a specific pipeline

        Args:
            pipeline_id: Pipeline UUID
            pipeline_number: Pipeline number (will search for ID)
            verbose: Include detailed job logs and error information

        Returns:
            Dictionary with complete pipeline information
        """
        try:
            # Get pipeline ID if only number provided
            if pipeline_number and not pipeline_id:
                pipeline = self.get_pipeline_by_number(pipeline_number)
                if not pipeline:
                    return None
                pipeline_id = pipeline.get("id")

            if not pipeline_id:
                self.logger.error("❌ Pipeline ID is required")
                return None

            self.logger.info(f"📊 Extracting details for pipeline: {pipeline_id}")

            # Get base pipeline information
            pipeline_data = self._make_request(f"/pipeline/{pipeline_id}")

            # Enhance with additional details
            detailed_pipeline = {
                "pipeline_info": pipeline_data,
                "workflows": self._get_pipeline_workflows(pipeline_id, verbose),
                "metadata": self._get_pipeline_metadata(pipeline_data),
                "analysis_timestamp": datetime.now().isoformat(),
                "project_slug": self.project_slug,
            }

            # Add trigger analysis
            detailed_pipeline["trigger_analysis"] = self._analyze_trigger(pipeline_data)

            # Add flaky tests analysis
            detailed_pipeline["flaky_tests_analysis"] = self.get_flaky_tests_analysis()

            # Generate intelligent failure summary
            detailed_pipeline["failure_summary"] = self._generate_failure_summary(detailed_pipeline)

            self.logger.info("✅ Pipeline details extracted successfully")
            return detailed_pipeline

        except Exception as e:
            self.logger.error(f"❌ Error getting pipeline details: {e}")
            return None

    def _get_pipeline_workflows(self, pipeline_id: str, verbose: bool = False) -> list[dict]:
        """Get all workflows for a pipeline with detailed information"""
        try:
            self.logger.info(f"🔄 Getting workflows for pipeline: {pipeline_id}")

            workflow_data = self._make_request(f"/pipeline/{pipeline_id}/workflow")
            workflows = []

            for workflow in workflow_data.get("items", []):
                workflow_detail = {
                    "id": workflow.get("id"),
                    "name": workflow.get("name"),
                    "status": workflow.get("status"),
                    "created_at": workflow.get("created_at"),
                    "stopped_at": workflow.get("stopped_at"),
                    "project_slug": workflow.get("project_slug"),
                    "tag": workflow.get("tag"),
                }

                # Calculate duration
                if workflow.get("stopped_at") and workflow.get("created_at"):
                    start = datetime.fromisoformat(workflow["created_at"].replace("Z", "+00:00"))
                    stop = datetime.fromisoformat(workflow["stopped_at"].replace("Z", "+00:00"))
                    workflow_detail["duration_seconds"] = int((stop - start).total_seconds())

                # Get jobs for this workflow
                workflow_detail["jobs"] = self._get_workflow_jobs(workflow["id"], verbose)

                workflows.append(workflow_detail)

                # Rate limiting
                time.sleep(0.1)

            self.logger.info(f"✅ Found {len(workflows)} workflows")
            return workflows

        except Exception as e:
            self.logger.error(f"❌ Error getting workflows: {e}")
            return []

    def _get_workflow_jobs(self, workflow_id: str, verbose: bool = False) -> list[dict]:
        """Get all jobs for a workflow with detailed information"""
        try:
            job_data = self._make_request(f"/workflow/{workflow_id}/job")
            jobs = []

            for job in job_data.get("items", []):
                job_detail = {
                    "id": job.get("id"),
                    "name": job.get("name"),
                    "status": job.get("status"),
                    "job_number": job.get("job_number"),
                    "type": job.get("type"),
                    "started_at": job.get("started_at"),
                    "stopped_at": job.get("stopped_at"),
                }

                # Calculate duration
                if job.get("stopped_at") and job.get("started_at"):
                    start = datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
                    stop = datetime.fromisoformat(job["stopped_at"].replace("Z", "+00:00"))
                    job_detail["duration_seconds"] = int((stop - start).total_seconds())

                # Get detailed job information if verbose
                if verbose:
                    try:
                        job_detail["details"] = self._get_job_details(job["job_number"])
                    except Exception as e:
                        self.logger.warning(f"⚠️  Could not get details for job {job['job_number']}: {e}")
                        job_detail["details"] = {}

                jobs.append(job_detail)

                # Rate limiting
                time.sleep(0.05)

            return jobs

        except Exception as e:
            self.logger.error(f"❌ Error getting jobs for workflow {workflow_id}: {e}")
            return []

    def _get_job_details(self, job_number: int) -> dict:
        """Get detailed job information including steps and logs"""
        try:
            job_detail = self._make_request(f"/project/{self.project_slug}/job/{job_number}")

            # Simplify the response to include key information
            basic_details = {
                "executor": job_detail.get("executor"),
                "parallelism": job_detail.get("parallelism"),
                "resource_class": job_detail.get("resource_class"),
                "web_url": job_detail.get("web_url"),
                "steps_count": len(job_detail.get("steps", [])),
                "has_artifacts": len(job_detail.get("artifacts", [])) > 0,
                "latest_workflow": job_detail.get("latest_workflow", {}).get("name"),
                "contexts": job_detail.get("contexts", []),
            }

            # Add detailed failure analysis for failed jobs
            if job_detail.get("status") == "failed":
                basic_details["failure_analysis"] = self._get_job_failure_analysis(job_number, job_detail)

            return basic_details

        except Exception as e:
            self.logger.warning(f"⚠️  Could not get details for job {job_number}: {e}")
            return {}

    def _get_pipeline_metadata(self, pipeline_data: dict) -> dict:
        """Extract and organize pipeline metadata"""
        vcs_info = pipeline_data.get("vcs", {})
        trigger_info = pipeline_data.get("trigger", {})

        return {
            "pipeline_number": pipeline_data.get("number"),
            "created_at": pipeline_data.get("created_at"),
            "updated_at": pipeline_data.get("updated_at"),
            "state": pipeline_data.get("state"),
            "git_info": {
                "revision": vcs_info.get("revision"),
                "branch": vcs_info.get("branch"),
                "tag": vcs_info.get("tag"),
                "commit_url": vcs_info.get("commit", {}).get("url"),
                "commit_subject": vcs_info.get("commit", {}).get("subject"),
                "commit_author": vcs_info.get("commit", {}).get("author", {}).get("name"),
            },
            "trigger_info": trigger_info,
        }

    def _analyze_trigger(self, pipeline_data: dict) -> dict:
        """Analyze what triggered the pipeline"""
        trigger = pipeline_data.get("trigger", {})
        vcs = pipeline_data.get("vcs", {})

        analysis = {
            "trigger_type": trigger.get("type"),
            "trigger_received_at": trigger.get("received_at"),
            "is_scheduled": trigger.get("type") == "scheduled_pipeline",
            "is_api": trigger.get("type") == "api",
            "is_webhook": trigger.get("type") == "webhook",
            "branch": vcs.get("branch"),
            "tag": vcs.get("tag"),
            "is_tag_trigger": bool(vcs.get("tag")),
            "is_main_branch": vcs.get("branch") in ["main", "master", "develop"],
            "commit_message": vcs.get("commit", {}).get("subject", ""),
            "actor": trigger.get("actor", {}).get("login") or trigger.get("actor", {}).get("name"),
        }

        # Determine likely trigger reason
        if analysis["is_tag_trigger"]:
            analysis["likely_reason"] = f"Tag release: {vcs.get('tag')}"
        elif analysis["trigger_type"] == "webhook" and analysis["branch"]:
            analysis["likely_reason"] = f"Push to branch: {analysis['branch']}"
        elif analysis["trigger_type"] == "api":
            analysis["likely_reason"] = "Manual API trigger or scheduled pipeline"
        else:
            analysis["likely_reason"] = f"Unknown trigger type: {analysis['trigger_type']}"

        return analysis

    def _get_job_failure_analysis(self, job_number: int, job_detail: dict) -> dict:
        """Get detailed failure analysis for a failed job"""
        try:
            self.logger.debug(f"🔍 Analyzing failure for job {job_number}")
            analysis: dict[str, Any] = {}

            # Get test metadata
            analysis["test_results"] = self._get_job_test_metadata(job_number)

            # Get artifacts (logs, reports)
            analysis["artifacts"] = self._get_job_artifacts(job_number)

            # Get detailed steps with failure points
            analysis["failed_steps"] = self._identify_failed_steps(job_detail.get("steps", []))

            # Calculate failure timing
            analysis["failure_timing"] = self._analyze_failure_timing(job_detail)

            return analysis

        except Exception as e:
            self.logger.warning(f"⚠️  Could not analyze failure for job {job_number}: {e}")
            return {}

    def _get_job_test_metadata(self, job_number: int) -> dict:
        """Get test results and analyze failed tests"""
        try:
            test_data = self._make_request(f"/project/{self.project_slug}/job/{job_number}/test-metadata")
            return self._analyze_test_failures(test_data)
        except Exception as e:
            self.logger.debug(f"No test metadata available for job {job_number}: {e}")
            return {}

    def _analyze_test_failures(self, test_data: dict) -> dict:
        """Analyze failed tests with detailed information"""
        if not test_data or not test_data.get("items"):
            return {}

        failed_tests = []
        test_summary: dict[str, Any] = {
            "total_tests": len(test_data.get("items", [])),
            "failed_count": 0,
            "passed_count": 0,
            "skipped_count": 0,
        }

        for test in test_data.get("items", []):
            result = test.get("result", "").lower()
            if result == "failure":
                failed_tests.append(
                    {
                        "name": test.get("name"),
                        "classname": test.get("classname"),
                        "file": test.get("file"),
                        "message": test.get("message"),
                        "run_time": test.get("run_time"),
                    }
                )
                test_summary["failed_count"] += 1
            elif result == "success":
                test_summary["passed_count"] += 1
            else:
                test_summary["skipped_count"] += 1

        return {
            "summary": test_summary,
            "failed_tests": failed_tests,
            "failure_patterns": self._identify_test_failure_patterns(failed_tests),
        }

    def _identify_test_failure_patterns(self, failed_tests: list[dict]) -> dict:
        """Identify common patterns in test failures"""
        patterns: dict[str, list[str]] = {
            "timeout_failures": [],
            "assertion_failures": [],
            "connection_failures": [],
            "other_failures": [],
        }

        for test in failed_tests:
            message = test.get("message", "").lower()
            if "timeout" in message or "timed out" in message:
                patterns["timeout_failures"].append(test["name"])
            elif "assert" in message or "expected" in message:
                patterns["assertion_failures"].append(test["name"])
            elif "connection" in message or "network" in message or "unreachable" in message:
                patterns["connection_failures"].append(test["name"])
            else:
                patterns["other_failures"].append(test["name"])

        return patterns

    def _get_job_artifacts(self, job_number: int) -> dict:
        """Get and categorize job artifacts for failure investigation"""
        try:
            artifacts_data = self._make_request(f"/project/{self.project_slug}/job/{job_number}/artifacts")
            return self._categorize_artifacts(artifacts_data)
        except Exception as e:
            self.logger.debug(f"No artifacts available for job {job_number}: {e}")
            return {}

    def _categorize_artifacts(self, artifacts_data: dict) -> dict:
        """Categorize and analyze artifacts for failure investigation"""
        categorized: dict[str, Any] = {
            "logs": [],
            "test_reports": [],
            "coverage_reports": [],
            "screenshots": [],
            "other": [],
            "total_count": 0,
        }

        if not artifacts_data or not artifacts_data.get("items"):
            return categorized

        for artifact in artifacts_data.get("items", []):
            path = artifact.get("path", "").lower()
            url = artifact.get("url")
            artifact_info = {"path": artifact.get("path"), "url": url}

            categorized["total_count"] += 1

            if any(pattern in path for pattern in [".log", ".txt", "stdout", "stderr"]):
                categorized["logs"].append(artifact_info)
            elif any(pattern in path for pattern in [".xml", ".json", ".html"]) and "test" in path:
                categorized["test_reports"].append(artifact_info)
            elif "coverage" in path:
                categorized["coverage_reports"].append(artifact_info)
            elif any(ext in path for ext in [".png", ".jpg", ".jpeg", ".gif"]):
                categorized["screenshots"].append(artifact_info)
            else:
                categorized["other"].append(artifact_info)

        return categorized

    def _identify_failed_steps(self, steps: list[dict]) -> list[dict]:
        """Identify which specific steps failed and why"""
        failed_steps = []

        for i, step in enumerate(steps):
            for action in step.get("actions", []):
                if action.get("status") == "failed":
                    step_info = {
                        "step_number": i + 1,
                        "name": action.get("name"),
                        "exit_code": action.get("exit_code"),
                        "start_time": action.get("start_time"),
                        "end_time": action.get("end_time"),
                        "has_output": bool(action.get("has_output")),
                        "output_url": action.get("output_url"),
                    }

                    # Add command info if it's a run step
                    if step.get("run"):
                        step_info["command"] = step["run"].get("command")
                        step_info["shell"] = step["run"].get("shell")

                    failed_steps.append(step_info)

        return failed_steps

    def _analyze_failure_timing(self, job_detail: dict) -> dict:
        """Analyze when the failure occurred in the job lifecycle"""
        timing = {
            "job_duration_seconds": 0,
            "time_to_failure": 0,
            "failure_stage": "unknown",
        }

        started_at = job_detail.get("started_at")
        stopped_at = job_detail.get("stopped_at")

        if started_at and stopped_at:
            start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            stop = datetime.fromisoformat(stopped_at.replace("Z", "+00:00"))
            timing["job_duration_seconds"] = int((stop - start).total_seconds())

        # Analyze steps to find when failure occurred
        steps = job_detail.get("steps", [])
        total_steps = len(steps)
        failed_step_index = -1

        for i, step in enumerate(steps):
            for action in step.get("actions", []):
                if action.get("status") == "failed":
                    failed_step_index = i
                    break
            if failed_step_index >= 0:
                break

        if failed_step_index >= 0 and total_steps > 0:
            failure_percentage = (failed_step_index / total_steps) * 100
            if failure_percentage < 25:
                timing["failure_stage"] = "early"
            elif failure_percentage < 75:
                timing["failure_stage"] = "middle"
            else:
                timing["failure_stage"] = "late"

        return timing

    def get_flaky_tests_analysis(self) -> dict:
        """Get flaky tests that might be causing intermittent failures"""
        try:
            self.logger.debug(f"🔍 Getting flaky tests for project: {self.project_slug}")
            flaky_data = self._make_request(f"/insights/{self.project_slug}/flaky-tests")

            flaky_tests = flaky_data.get("flaky-tests", [])
            return {
                "flaky_tests_count": len(flaky_tests),
                "tests": flaky_tests[:10],  # Limit to top 10 most flaky
                "has_flaky_tests": len(flaky_tests) > 0,
            }
        except Exception as e:
            self.logger.debug(f"Could not fetch flaky tests: {e}")
            return {"flaky_tests_count": 0, "tests": [], "has_flaky_tests": False}

    def analyze_pipeline_config(self, pipeline_data: dict) -> dict:
        """Analyze the CircleCI configuration for this pipeline"""
        try:
            # This would require access to the .circleci/config.yml file
            # For now, we'll analyze based on the workflow patterns we see
            workflows = pipeline_data.get("workflows", [])

            analysis = {
                "total_workflows": len(workflows),
                "workflow_names": [w.get("name") for w in workflows],
                "workflow_statuses": {w.get("name"): w.get("status") for w in workflows},
                "triggered_workflows": [w.get("name") for w in workflows if w.get("status") != "not_run"],
                "failed_workflows": [w.get("name") for w in workflows if w.get("status") == "failed"],
                "successful_workflows": [w.get("name") for w in workflows if w.get("status") == "success"],
            }

            # Analyze job patterns
            all_jobs = []
            for workflow in workflows:
                for job in workflow.get("jobs", []):
                    all_jobs.append(
                        {
                            "workflow": workflow.get("name"),
                            "job_name": job.get("name"),
                            "status": job.get("status"),
                            "type": job.get("type"),
                        }
                    )

            analysis["job_analysis"] = {
                "total_jobs": len(all_jobs),
                "jobs_by_workflow": {w.get("name"): len(w.get("jobs", [])) for w in workflows},
                "job_statuses": {job["job_name"]: job["status"] for job in all_jobs},
                "failed_jobs": [job["job_name"] for job in all_jobs if job["status"] == "failed"],
            }

            return analysis

        except Exception as e:
            self.logger.error(f"❌ Error analyzing pipeline configuration: {e}")
            return {}

    def _generate_failure_summary(self, pipeline_data: dict) -> dict:
        """Generate intelligent failure summary with root cause analysis"""
        summary: dict[str, Any] = {
            "failure_type": "none",
            "likely_causes": [],
            "recommendations": [],
            "failed_jobs_count": 0,
            "failed_workflows_count": 0,
            "has_test_failures": False,
            "has_infrastructure_failures": False,
            "has_timeout_failures": False,
            "total_failed_tests": 0,
            "most_common_failure_pattern": "unknown",
        }

        all_failure_patterns: list[str] = []

        # Analyze workflow failures
        for workflow in pipeline_data.get("workflows", []):
            if workflow.get("status") == "failed":
                summary["failed_workflows_count"] += 1

            for job in workflow.get("jobs", []):
                if job.get("status") == "failed":
                    summary["failed_jobs_count"] += 1

                    # Analyze job failure details
                    failure_analysis = job.get("details", {}).get("failure_analysis", {})
                    if failure_analysis:
                        # Test failures
                        test_results = failure_analysis.get("test_results", {})
                        if test_results.get("failed_tests"):
                            summary["has_test_failures"] = True
                            summary["total_failed_tests"] += len(test_results["failed_tests"])

                            # Collect failure patterns
                            patterns = test_results.get("failure_patterns", {})
                            for pattern_type, tests in patterns.items():
                                if tests:
                                    all_failure_patterns.extend([pattern_type] * len(tests))

                        # Infrastructure failures
                        failed_steps = failure_analysis.get("failed_steps", [])
                        for step in failed_steps:
                            if step.get("exit_code") in [
                                125,
                                126,
                                127,
                            ]:  # Common infrastructure exit codes
                                summary["has_infrastructure_failures"] = True

                            # Check for timeout in step names/commands
                            step_name = step.get("name", "").lower()
                            step_command = step.get("command", "").lower()
                            if "timeout" in step_name or "timeout" in step_command:
                                summary["has_timeout_failures"] = True

                # Check job type for infrastructure failures
                elif job.get("status") == "infrastructure_fail":
                    summary["has_infrastructure_failures"] = True
                    summary["failed_jobs_count"] += 1

        # Determine most common failure pattern
        if all_failure_patterns:
            from collections import Counter

            pattern_counts = Counter(all_failure_patterns)
            summary["most_common_failure_pattern"] = pattern_counts.most_common(1)[0][0]

        # Determine overall failure type
        if summary["has_test_failures"]:
            summary["failure_type"] = "test_failures"
        elif summary["has_infrastructure_failures"]:
            summary["failure_type"] = "infrastructure_failures"
        elif summary["has_timeout_failures"]:
            summary["failure_type"] = "timeout_failures"
        elif summary["failed_jobs_count"] > 0:
            summary["failure_type"] = "build_failures"

        # Generate likely causes based on analysis
        if summary["has_test_failures"]:
            summary["likely_causes"].append(f"{summary['total_failed_tests']} test(s) failed")
            if summary["most_common_failure_pattern"] != "unknown":
                summary["likely_causes"].append(
                    f"Most common pattern: {summary['most_common_failure_pattern'].replace('_', ' ')}"
                )

        if summary["has_infrastructure_failures"]:
            summary["likely_causes"].append("Infrastructure or resource allocation issues")

        if summary["has_timeout_failures"]:
            summary["likely_causes"].append("Timeout issues - jobs or tests taking too long")

        # Generate recommendations
        if summary["failed_jobs_count"] > 0:
            summary["recommendations"].append(f"Check detailed logs for {summary['failed_jobs_count']} failed job(s)")

        if summary["has_test_failures"]:
            summary["recommendations"].append("Review failed test results and fix failing assertions")
            if "timeout_failures" in [summary["most_common_failure_pattern"]]:
                summary["recommendations"].append("Consider increasing test timeouts or optimizing slow tests")

        if summary["has_infrastructure_failures"]:
            summary["recommendations"].append("Check resource class allocation and CircleCI service status")

        # Add flaky tests info if available
        flaky_tests = pipeline_data.get("flaky_tests_analysis", {})
        if flaky_tests.get("has_flaky_tests"):
            summary["likely_causes"].append(f"{flaky_tests.get('flaky_tests_count', 0)} flaky test(s) detected")
            summary["recommendations"].append("Consider stabilizing flaky tests to reduce intermittent failures")

        return summary

    def save_to_file(self, data: dict, file_path: str):
        """Save pipeline data to a JSON file"""
        try:
            JSONManager.write_json(data, file_path)
            self.logger.info(f"✅ Data saved to: {file_path}")
        except Exception as e:
            self.logger.error(f"❌ Error saving to file: {e}")
            raise

    def print_pipeline_summary(self, pipeline_data: dict, verbose: bool = False):
        """Print a formatted summary of the pipeline data"""
        pipeline_info = pipeline_data.get("pipeline_info", {})
        metadata = pipeline_data.get("metadata", {})
        workflows = pipeline_data.get("workflows", [])
        trigger_analysis = pipeline_data.get("trigger_analysis", {})
        failure_summary = pipeline_data.get("failure_summary", {})
        flaky_tests = pipeline_data.get("flaky_tests_analysis", {})

        print("\n" + "=" * 80)
        print("📊 CIRCLECI PIPELINE ANALYSIS")
        print("=" * 80)

        # Show failure summary at the top if there are failures
        if failure_summary.get("failure_type") != "none":
            print("\n🚨 FAILURE SUMMARY:")
            print(f"   Failure Type: {failure_summary.get('failure_type', 'unknown').replace('_', ' ').title()}")
            print(f"   Failed Workflows: {failure_summary.get('failed_workflows_count', 0)}")
            print(f"   Failed Jobs: {failure_summary.get('failed_jobs_count', 0)}")

            if failure_summary.get("total_failed_tests", 0) > 0:
                print(f"   Failed Tests: {failure_summary.get('total_failed_tests', 0)}")

            if failure_summary.get("likely_causes"):
                print("   Likely Causes:")
                for cause in failure_summary["likely_causes"]:
                    print(f"     • {cause}")

            if failure_summary.get("recommendations"):
                print("   Recommendations:")
                for rec in failure_summary["recommendations"]:
                    print(f"     • {rec}")
        else:
            print("\n✅ PIPELINE STATUS: All workflows completed successfully")

        # Basic Information
        print("\n🔍 PIPELINE INFORMATION:")
        print(f"   Pipeline Number: {metadata.get('pipeline_number')}")
        print(f"   Pipeline ID: {pipeline_info.get('id')}")
        print(f"   Project: {self.project_slug}")
        print(f"   State: {metadata.get('state')}")
        print(f"   Created: {metadata.get('created_at')}")

        # Git Information
        git_info = metadata.get("git_info", {})
        print("\n📝 GIT INFORMATION:")
        print(f"   Branch: {git_info.get('branch')}")
        print(f"   Tag: {git_info.get('tag') or 'None'}")
        print(f"   Commit: {git_info.get('revision', '')[:8]}...")
        print(f"   Author: {git_info.get('commit_author')}")
        print(f"   Message: {git_info.get('commit_subject')}")

        # Trigger Analysis
        print("\n🚀 TRIGGER ANALYSIS:")
        print(f"   Trigger Type: {trigger_analysis.get('trigger_type')}")
        print(f"   Likely Reason: {trigger_analysis.get('likely_reason')}")
        print(f"   Actor: {trigger_analysis.get('actor')}")
        print(f"   Is Tag Trigger: {trigger_analysis.get('is_tag_trigger')}")

        # Detailed Workflows Analysis
        print("\n⚙️  DETAILED WORKFLOWS ANALYSIS:")
        print(f"   Total Workflows Found: {len(workflows)}")

        for i, workflow in enumerate(workflows, 1):
            try:
                status_emoji = {
                    "success": "✅",
                    "failed": "❌",
                    "running": "🔄",
                    "canceled": "⏹️",
                    "not_run": "⏸️",
                    "failing": "🔶",
                    "on_hold": "⏸️",
                }.get(workflow.get("status"), "❓")

                print(f"\n   📋 WORKFLOW {i}: {workflow.get('name')}")
                print(f"      Status: {status_emoji} {workflow.get('status')}")
                print(f"      ID: {workflow.get('id')}")
                print(f"      Created: {workflow.get('created_at')}")
                print(f"      Stopped: {workflow.get('stopped_at') or 'N/A'}")

                if workflow.get("duration_seconds"):
                    duration = workflow["duration_seconds"]
                    print(f"      Duration: {duration // 60}m {duration % 60}s")

                # Tag information if available
                if workflow.get("tag"):
                    print(f"      Tag: {workflow.get('tag')}")

                # Jobs Analysis
                jobs = workflow.get("jobs", [])
                print(f"      Jobs ({len(jobs)}):")

                if not jobs:
                    print("        ⚠️  No jobs found (workflow may have been canceled before jobs started)")
                else:
                    for j, job in enumerate(jobs, 1):
                        try:
                            job_emoji = {
                                "success": "✅",
                                "failed": "❌",
                                "running": "🔄",
                                "canceled": "⏹️",
                                "not_run": "⏸️",
                                "blocked": "🚫",
                                "infrastructure_fail": "🔥",
                            }.get(job.get("status"), "❓")

                            job_duration = ""
                            if job.get("duration_seconds"):
                                d = job["duration_seconds"]
                                job_duration = f" ({d // 60}m {d % 60}s)"

                            print(f"        {job_emoji} {job.get('name')}: {job.get('status')}{job_duration}")
                            print(f"           Job Number: {job.get('job_number')}")
                            print(f"           Type: {job.get('type', 'build')}")

                            if job.get("started_at"):
                                print(f"           Started: {job.get('started_at')}")
                            if job.get("stopped_at"):
                                print(f"           Stopped: {job.get('stopped_at')}")

                            # Show detailed job info if verbose
                            if verbose and job.get("details"):
                                details = job["details"]
                                print("           Details:")
                                if details.get("executor"):
                                    print(f"             Executor: {details['executor']}")
                                if details.get("resource_class"):
                                    print(f"             Resource Class: {details['resource_class']}")
                                if details.get("parallelism"):
                                    print(f"             Parallelism: {details['parallelism']}")
                                if details.get("steps_count"):
                                    print(f"             Steps: {details['steps_count']}")
                                if details.get("has_artifacts"):
                                    print(f"             Has Artifacts: {details['has_artifacts']}")
                                if details.get("contexts"):
                                    # Handle contexts properly - they might be dicts or strings
                                    contexts = details["contexts"]
                                    if isinstance(contexts, list):
                                        context_names = []
                                        for ctx in contexts:
                                            if isinstance(ctx, dict):
                                                context_names.append(ctx.get("name", str(ctx)))
                                            else:
                                                context_names.append(str(ctx))
                                        print(f"             Contexts: {', '.join(context_names)}")
                                    else:
                                        print(f"             Contexts: {contexts}")
                                if details.get("web_url"):
                                    print(f"             URL: {details['web_url']}")

                                # Show failure analysis if job failed
                                failure_analysis = details.get("failure_analysis", {})
                                if failure_analysis:
                                    print("           🔍 Failure Analysis:")

                                    # Test results
                                    test_results = failure_analysis.get("test_results", {})
                                    if test_results.get("summary"):
                                        summary = test_results["summary"]
                                        print(
                                            f"             Tests: {summary.get('failed_count', 0)}/{summary.get('total_tests', 0)} failed"
                                        )

                                        if test_results.get("failed_tests"):
                                            print("             Failed Tests:")
                                            for test in test_results["failed_tests"][:3]:  # Show first 3
                                                print(f"               • {test.get('name', 'Unknown')}")
                                                if test.get("message"):
                                                    msg = (
                                                        test["message"][:100] + "..."
                                                        if len(test["message"]) > 100
                                                        else test["message"]
                                                    )
                                                    print(f"                 Error: {msg}")
                                            if len(test_results["failed_tests"]) > 3:
                                                print(
                                                    f"               ... and {len(test_results['failed_tests']) - 3} more"
                                                )

                                    # Failed steps
                                    failed_steps = failure_analysis.get("failed_steps", [])
                                    if failed_steps:
                                        print(f"             Failed Steps: {len(failed_steps)}")
                                        for step in failed_steps[:2]:  # Show first 2 steps
                                            print(
                                                f"               • Step {step.get('step_number', '?')}: {step.get('name', 'Unknown')}"
                                            )
                                            if step.get("exit_code"):
                                                print(f"                 Exit Code: {step['exit_code']}")
                                            if step.get("command"):
                                                cmd = (
                                                    step["command"][:80] + "..."
                                                    if len(step["command"]) > 80
                                                    else step["command"]
                                                )
                                                print(f"                 Command: {cmd}")
                                        if len(failed_steps) > 2:
                                            print(f"               ... and {len(failed_steps) - 2} more steps")

                                    # Artifacts
                                    artifacts = failure_analysis.get("artifacts", {})
                                    if artifacts.get("total_count", 0) > 0:
                                        print(f"             Artifacts: {artifacts['total_count']} total")
                                        if artifacts.get("logs"):
                                            print(f"               Logs: {len(artifacts['logs'])}")
                                        if artifacts.get("test_reports"):
                                            print(f"               Test Reports: {len(artifacts['test_reports'])}")
                                        if artifacts.get("screenshots"):
                                            print(f"               Screenshots: {len(artifacts['screenshots'])}")

                                    # Failure timing
                                    timing = failure_analysis.get("failure_timing", {})
                                    if timing.get("failure_stage") != "unknown":
                                        print(f"             Failure Stage: {timing['failure_stage']} in job lifecycle")
                        except Exception as e:
                            print(f"        ❌ Error displaying job {j}: {e}")
                            self.logger.error(f"Error displaying job {j}: {e}")

            except Exception as e:
                print(f"   ❌ Error displaying workflow {i}: {e}")
                self.logger.error(f"Error displaying workflow {i}: {e}")

        # Configuration Analysis
        config_analysis = pipeline_data.get("config_analysis", {})
        if config_analysis:
            print("\n🔧 CONFIGURATION ANALYSIS:")
            print(f"   Triggered Workflows: {', '.join(config_analysis.get('triggered_workflows', []))}")
            print(
                f"   Not Triggered: {', '.join(config_analysis.get('workflow_names', [])) if set(config_analysis.get('workflow_names', [])) - set(config_analysis.get('triggered_workflows', [])) else 'None'}"
            )
            print(
                f"   Failed Workflows: {', '.join(config_analysis.get('failed_workflows', []))} {'' if config_analysis.get('failed_workflows') else '(None)'}"
            )
            print(
                f"   Successful Workflows: {', '.join(config_analysis.get('successful_workflows', []))} {'' if config_analysis.get('successful_workflows') else '(None)'}"
            )
            print(f"   Total Jobs: {config_analysis.get('job_analysis', {}).get('total_jobs', 0)}")

            # Jobs by workflow breakdown
            jobs_by_workflow = config_analysis.get("job_analysis", {}).get("jobs_by_workflow", {})
            if jobs_by_workflow:
                print("   Jobs per Workflow:")
                for wf_name, job_count in jobs_by_workflow.items():
                    print(f"     - {wf_name}: {job_count} jobs")

            # Failed jobs
            failed_jobs = config_analysis.get("job_analysis", {}).get("failed_jobs", [])
            if failed_jobs:
                print(f"   Failed Jobs: {', '.join(failed_jobs)}")

        # Trigger Analysis Details
        print("\n🎯 TRIGGER PATTERN ANALYSIS:")
        tag = git_info.get("tag")
        branch = git_info.get("branch")

        if tag:
            print(f"   🏷️  Tag: '{tag}'")
        else:
            print("   🏷️  Tag: -")

        if branch:
            print(f"   🌿 Branch: '{branch}'")
        else:
            print("   🌿 Branch: -")

        print(f"   ⚡ Trigger Actor: {trigger_analysis.get('actor', 'Unknown')}")
        print(f"   🕐 Trigger Time: {trigger_analysis.get('trigger_received_at', 'Unknown')}")

        # Flaky Tests Analysis
        if flaky_tests.get("has_flaky_tests"):
            print("\n🔄 FLAKY TESTS ANALYSIS:")
            print(f"   Total Flaky Tests: {flaky_tests.get('flaky_tests_count', 0)}")
            print("   ⚠️  These tests have inconsistent pass/fail results and may cause intermittent pipeline failures")

            if verbose and flaky_tests.get("tests"):
                print("   Most Flaky Tests:")
                for i, test in enumerate(flaky_tests["tests"][:5], 1):  # Show top 5
                    test_name = test.get("test_name", "Unknown")
                    flake_rate = test.get("flake_rate", 0)
                    print(f"     {i}. {test_name} (flake rate: {flake_rate:.1%})")
        elif verbose:
            print("\n🔄 FLAKY TESTS ANALYSIS:")
            print("   ✅ No flaky tests detected in this project")

        print("\n" + "=" * 80)
