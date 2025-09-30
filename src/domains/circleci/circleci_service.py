"""
CircleCI Performance Analysis Service
Converts the Node.js CircleCI analysis script into Python service
"""

import json
import os
import requests
import time
from datetime import datetime
from typing import Dict, List
import pandas as pd
import matplotlib.pyplot as plt

from utils.logging.logging_manager import LogManager


class CircleCIService:
    """Service for analyzing CircleCI pipeline performance data"""

    def __init__(self, token: str, project_slug: str = ""):
        """
        Initialize CircleCI service

        Args:
            token: CircleCI API token
            project_slug: Project slug (e.g., 'gh/org/repo'), optional for listing projects
        """
        self.token = token
        self.project_slug = project_slug
        self.base_url = "https://circleci.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update(
            {"Circle-Token": token, "Accept": "application/json"}
        )
        self.logger = LogManager.get_instance().get_logger("CircleCIService")

    def _make_request(self, endpoint: str) -> Dict:
        """Make API request to CircleCI"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to make request to {url}: {e}")
            raise

    def list_projects(self) -> List[Dict]:
        """
        List all projects accessible to the user by getting organizations and their pipelines.

        Returns:
            List of project information
        """
        self.logger.info("Listing CircleCI projects...")

        try:
            headers = {"Circle-Token": self.token, "Accept": "application/json"}

            # First get user's collaborations (organizations)
            collab_url = f"{self.base_url}/me/collaborations"
            self.logger.debug(f"Getting collaborations from: {collab_url}")

            collab_response = requests.get(collab_url, headers=headers, timeout=30)

            if collab_response.status_code != 200:
                self.logger.error(
                    f"Failed to get collaborations. Status: {collab_response.status_code}"
                )
                return []

            collaborations = collab_response.json()
            projects = {}  # Use dict to deduplicate by project_slug

            # For each organization, get recent pipelines to discover projects
            for org in collaborations:
                org_slug = org.get("slug", "")
                if not org_slug:
                    continue

                self.logger.debug(f"Getting pipelines for organization: {org_slug}")

                # Get pipelines for this org to discover projects
                pipeline_url = f"{self.base_url}/pipeline"
                params = {"org-slug": org_slug}

                pipeline_response = requests.get(
                    pipeline_url, headers=headers, params=params, timeout=30
                )

                if pipeline_response.status_code == 200:
                    pipeline_data = pipeline_response.json()

                    if "items" in pipeline_data:
                        for pipeline in pipeline_data["items"]:
                            project_slug = pipeline.get("project_slug", "")
                            if project_slug and project_slug not in projects:
                                # Extract project info from pipeline data
                                vcs_info = pipeline.get("vcs", {})
                                project_name = (
                                    project_slug.split("/")[-1]
                                    if "/" in project_slug
                                    else project_slug
                                )

                                # Determine VCS type from project_slug
                                vcs_type = (
                                    "GitHub"
                                    if project_slug.startswith("gh/")
                                    else "Bitbucket"
                                )

                                # Use main branch as default, or get from first pipeline
                                default_branch = "main"
                                if vcs_info.get("branch"):
                                    default_branch = vcs_info.get("branch")

                                projects[project_slug] = {
                                    "name": project_name,
                                    "organization": project_slug.split("/")[1]
                                    if "/" in project_slug
                                    else "Unknown",
                                    "slug": project_slug,
                                    "vcs_type": vcs_type,
                                    "url": vcs_info.get("target_repository_url", ""),
                                    "default_branch": default_branch,
                                }
                else:
                    self.logger.warning(
                        f"Failed to get pipelines for {org_slug}. Status: {pipeline_response.status_code}"
                    )

            # Convert dict values to list
            project_list = list(projects.values())

            self.logger.info(f"Found {len(project_list)} unique projects")
            return project_list

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error listing projects: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error listing projects: {e}")
            return []

    def export_pipelines(self, limit: int = 100) -> List[Dict]:
        """Export pipeline data"""
        self.logger.info("📊 Exporting pipeline data...")

        try:
            data = self._make_request(f"/project/{self.project_slug}/pipeline")

            if not data.get("items") or not isinstance(data["items"], list):
                self.logger.error("❌ No pipeline data found or invalid format")
                return []

            pipelines = []
            for pipeline in data["items"][:limit]:
                pipelines.append(
                    {
                        "id": pipeline.get("id"),
                        "number": pipeline.get("number"),
                        "state": pipeline.get("state"),
                        "created_at": pipeline.get("created_at"),
                        "updated_at": pipeline.get("updated_at"),
                        "vcs": {
                            "branch": pipeline.get("vcs", {}).get("branch"),
                            "commit": pipeline.get("vcs", {})
                            .get("commit", {})
                            .get("subject"),
                            "revision": pipeline.get("vcs", {}).get("revision"),
                        },
                        "trigger": pipeline.get("trigger"),
                    }
                )

            self.logger.info(f"✅ Exported {len(pipelines)} pipelines")
            return pipelines

        except Exception as e:
            self.logger.error(f"❌ Error exporting pipelines: {e}")
            return []

    def export_workflows(self, pipelines: List[Dict], limit: int = 20) -> List[Dict]:
        """Export workflow data for pipelines"""
        self.logger.info("🔄 Exporting workflow data...")

        all_workflows = []

        for pipeline in pipelines[:limit]:
            try:
                data = self._make_request(f"/pipeline/{pipeline['id']}/workflow")

                for workflow in data.get("items", []):
                    duration_seconds = None
                    if workflow.get("stopped_at") and workflow.get("created_at"):
                        start = datetime.fromisoformat(
                            workflow["created_at"].replace("Z", "+00:00")
                        )
                        stop = datetime.fromisoformat(
                            workflow["stopped_at"].replace("Z", "+00:00")
                        )
                        duration_seconds = int((stop - start).total_seconds())

                    all_workflows.append(
                        {
                            "id": workflow.get("id"),
                            "name": workflow.get("name"),
                            "status": workflow.get("status"),
                            "created_at": workflow.get("created_at"),
                            "stopped_at": workflow.get("stopped_at"),
                            "pipeline_id": pipeline["id"],
                            "pipeline_number": pipeline["number"],
                            "duration_seconds": duration_seconds,
                        }
                    )

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                self.logger.warning(
                    f"⚠️  Failed to get workflows for pipeline {pipeline['number']}: {e}"
                )

        self.logger.info(f"✅ Exported {len(all_workflows)} workflows")
        return all_workflows

    def export_jobs(self, workflows: List[Dict], limit: int = 10) -> List[Dict]:
        """Export job data for workflows"""
        self.logger.info("⚙️  Exporting job data...")

        all_jobs = []

        for workflow in workflows[:limit]:
            try:
                data = self._make_request(f"/workflow/{workflow['id']}/job")

                for job in data.get("items", []):
                    duration_seconds = None
                    if job.get("stopped_at") and job.get("started_at"):
                        start = datetime.fromisoformat(
                            job["started_at"].replace("Z", "+00:00")
                        )
                        stop = datetime.fromisoformat(
                            job["stopped_at"].replace("Z", "+00:00")
                        )
                        duration_seconds = int((stop - start).total_seconds())

                    all_jobs.append(
                        {
                            "id": job.get("id"),
                            "name": job.get("name"),
                            "status": job.get("status"),
                            "started_at": job.get("started_at"),
                            "stopped_at": job.get("stopped_at"),
                            "workflow_id": workflow["id"],
                            "workflow_name": workflow["name"],
                            "pipeline_number": workflow["pipeline_number"],
                            "duration_seconds": duration_seconds,
                            "resource_class": None,  # Would need to extract from config
                        }
                    )

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                self.logger.warning(
                    f"⚠️  Failed to get jobs for workflow {workflow['id']}: {e}"
                )

        self.logger.info(f"✅ Exported {len(all_jobs)} jobs")
        return all_jobs

    def generate_analysis(
        self, pipelines: List[Dict], workflows: List[Dict], jobs: List[Dict]
    ) -> Dict:
        """Generate performance analysis"""
        self.logger.info("📈 Generating performance analysis...")

        # Calculate workflow averages
        valid_workflows = [w for w in workflows if w.get("duration_seconds")]
        avg_duration = (
            sum(w["duration_seconds"] for w in valid_workflows) / len(valid_workflows)
            if valid_workflows
            else 0
        )

        success_workflows = [w for w in workflows if w.get("status") == "success"]
        failed_workflows = [w for w in workflows if w.get("status") == "failed"]

        success_rate = (
            (len(success_workflows) / len(workflows) * 100) if workflows else 0
        )
        failure_rate = (
            (len(failed_workflows) / len(workflows) * 100) if workflows else 0
        )

        # Calculate job performance
        job_performance = {}
        valid_jobs = [j for j in jobs if j.get("duration_seconds")]

        for job in valid_jobs:
            job_name = job["name"]
            if job_name not in job_performance:
                job_performance[job_name] = {
                    "total_runs": 0,
                    "total_duration": 0,
                    "avg_duration": 0,
                    "success_rate": 0,
                    "successes": 0,
                }

            job_performance[job_name]["total_runs"] += 1
            job_performance[job_name]["total_duration"] += job["duration_seconds"]
            job_performance[job_name]["avg_duration"] = round(
                job_performance[job_name]["total_duration"]
                / job_performance[job_name]["total_runs"]
            )

            if job.get("status") == "success":
                job_performance[job_name]["successes"] += 1

            job_performance[job_name]["success_rate"] = round(
                (
                    job_performance[job_name]["successes"]
                    / job_performance[job_name]["total_runs"]
                )
                * 100
            )

        # Find slowest jobs
        slowest_jobs = sorted(
            valid_jobs, key=lambda x: x["duration_seconds"], reverse=True
        )[:10]
        slowest_jobs_data = [
            {
                "name": job["name"],
                "duration_seconds": job["duration_seconds"],
                "duration_minutes": round(job["duration_seconds"] / 60, 2),
                "workflow": job["workflow_name"],
                "pipeline": job["pipeline_number"],
            }
            for job in slowest_jobs
        ]

        analysis = {
            "summary": {
                "total_pipelines": len(pipelines),
                "total_workflows": len(workflows),
                "total_jobs": len(jobs),
                "generated_at": datetime.now().isoformat(),
            },
            "pipeline_performance": {
                "avg_duration": round(avg_duration),
                "success_rate": round(success_rate),
                "failure_rate": round(failure_rate),
            },
            "job_performance": job_performance,
            "slowest_jobs": slowest_jobs_data,
        }

        self.logger.info("✅ Performance analysis generated")
        return analysis

    def generate_recommendations(self, analysis: Dict) -> List[Dict]:
        """Generate optimization recommendations"""
        recommendations: List[Dict] = []
        job_perf = analysis["job_performance"]

        for job_name, metrics in job_perf.items():
            if metrics["avg_duration"] > 120:  # > 2 minutes
                recommendations.append(
                    {
                        "type": "performance",
                        "job": job_name,
                        "issue": f"Long execution time ({round(metrics['avg_duration'] / 60)}m)",
                        "suggestion": "Consider increasing parallelism or resource class",
                    }
                )

            if metrics["success_rate"] < 95:
                recommendations.append(
                    {
                        "type": "reliability",
                        "job": job_name,
                        "issue": f"Low success rate ({metrics['success_rate']}%)",
                        "suggestion": "Investigate flaky tests or resource constraints",
                    }
                )

        return recommendations

    def identify_bottlenecks(self, analysis: Dict) -> List[Dict]:
        """Identify performance bottlenecks"""
        self.logger.info("🔍 Identifying bottlenecks...")

        bottlenecks = []
        job_perf = analysis["job_performance"]

        # Sort jobs by average duration
        sorted_jobs = sorted(
            job_perf.items(), key=lambda x: x[1]["avg_duration"], reverse=True
        )

        for job_name, metrics in sorted_jobs[:5]:  # Top 5 slowest
            bottlenecks.append(
                {
                    "job": job_name,
                    "avg_duration_minutes": round(metrics["avg_duration"] / 60, 2),
                    "success_rate": metrics["success_rate"],
                    "total_runs": metrics["total_runs"],
                    "optimization_potential": "HIGH"
                    if metrics["avg_duration"] > 180
                    else "MEDIUM",
                }
            )

        return bottlenecks

    def generate_optimization_plan(
        self, bottlenecks: List[Dict], analysis: Dict
    ) -> Dict:
        """Generate detailed optimization plan"""
        self.logger.info("🛠️  Generating optimization plan...")

        # Initialize with explicit types
        immediate_actions: List[Dict] = []
        medium_term: List[Dict] = []

        plan = {
            "immediate_actions": immediate_actions,
            "medium_term": medium_term,
            "long_term": [],
            "estimated_savings": 0,
        }

        total_pipeline_time = analysis["pipeline_performance"]["avg_duration"]

        for bottleneck in bottlenecks:
            job = bottleneck["job"]
            duration = bottleneck["avg_duration_minutes"]

            if "install-dependencies" in job.lower():
                immediate_actions.append(
                    {
                        "action": "Optimize dependency caching",
                        "job": job,
                        "current_time": f"{duration}m",
                        "estimated_savings": "30-50%",
                        "implementation": "Enhanced cache keys with branch fallback",
                    }
                )

            elif "sonarcloud" in job.lower():
                medium_term.append(
                    {
                        "action": "Optimize SonarCloud analysis",
                        "job": job,
                        "current_time": f"{duration}m",
                        "estimated_savings": "20-30%",
                        "implementation": "Increase resource class, enable parallel analysis",
                    }
                )

            elif "build" in job.lower():
                immediate_actions.append(
                    {
                        "action": "Increase build parallelism",
                        "job": job,
                        "current_time": f"{duration}m",
                        "estimated_savings": "25-40%",
                        "implementation": "Increase parallelism from 10 to 15, upgrade resource class",
                    }
                )

            elif "test" in job.lower():
                medium_term.append(
                    {
                        "action": "Optimize test execution",
                        "job": job,
                        "current_time": f"{duration}m",
                        "estimated_savings": "15-25%",
                        "implementation": "Better test splitting, cache test dependencies",
                    }
                )

        # Calculate estimated total savings
        immediate_savings = (
            0.3 * total_pipeline_time / len(immediate_actions)
            if immediate_actions
            else 0
        )
        medium_savings = (
            0.25 * total_pipeline_time / len(medium_term) if medium_term else 0
        )

        plan["estimated_savings"] = round(immediate_savings + medium_savings)
        plan["estimated_final_time"] = round(
            total_pipeline_time - plan["estimated_savings"]
        )

        return plan

    def create_visualizations(
        self, workflows: List[Dict], jobs: List[Dict], output_path: str
    ) -> bool:
        """Create performance visualizations"""
        self.logger.info("📊 Creating visualizations...")

        try:
            # Set a clean style
            plt.style.use("default")
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))

            # Job duration distribution
            df_jobs = pd.DataFrame([j for j in jobs if j.get("duration_seconds")])

            if not df_jobs.empty:
                df_jobs["duration_seconds"].hist(bins=20, ax=axes[0, 0])
                axes[0, 0].set_title("Job Duration Distribution")
                axes[0, 0].set_xlabel("Duration (seconds)")

                # Job performance by name
                job_perf = (
                    df_jobs.groupby("name")["duration_seconds"]
                    .mean()
                    .sort_values(ascending=False)
                    .head(8)
                )
                job_perf.plot(kind="bar", ax=axes[0, 1])
                axes[0, 1].set_title("Average Job Duration by Name")
                axes[0, 1].set_ylabel("Duration (seconds)")
                axes[0, 1].tick_params(axis="x", rotation=45)

                # Success rate by job
                success_rates = (
                    df_jobs.groupby("name")
                    .apply(lambda x: (x["status"] == "success").mean() * 100)
                    .sort_values(ascending=False)
                )
                success_rates.plot(kind="bar", ax=axes[1, 0], color="green")
                axes[1, 0].set_title("Success Rate by Job")
                axes[1, 0].set_ylabel("Success Rate (%)")
                axes[1, 0].tick_params(axis="x", rotation=45)

                # Timeline of builds
                df_jobs["started_at"] = pd.to_datetime(df_jobs["started_at"])
                daily_builds = df_jobs.groupby(df_jobs["started_at"].dt.date).size()
                daily_builds.plot(ax=axes[1, 1])
                axes[1, 1].set_title("Builds per Day")
                axes[1, 1].set_ylabel("Number of Builds")

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close(fig)  # Clean up memory
            self.logger.info(f"✅ Charts saved to: {output_path}")
            return True

        except Exception as e:
            self.logger.warning(f"⚠️  Could not create visualizations: {e}")
            return False

    def save_data(
        self,
        output_dir: str,
        pipelines: List[Dict],
        workflows: List[Dict],
        jobs: List[Dict],
        analysis: Dict,
        recommendations: List[Dict],
    ) -> None:
        """Save all data to files"""
        os.makedirs(output_dir, exist_ok=True)

        files_to_save = {
            "pipelines.json": pipelines,
            "workflows.json": workflows,
            "jobs.json": jobs,
            "analysis.json": analysis,
            "recommendations.json": recommendations,
        }

        for filename, data in files_to_save.items():
            filepath = os.path.join(output_dir, filename)
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"✅ Saved {filename}")

    def run_complete_analysis(self, output_dir: str = "./circleci-analysis") -> Dict:
        """Run complete CircleCI performance analysis"""
        self.logger.info("🚀 Starting CircleCI performance analysis...")

        # Export data
        pipelines = self.export_pipelines()
        workflows = self.export_workflows(pipelines)
        jobs = self.export_jobs(workflows)

        # Generate analysis
        analysis = self.generate_analysis(pipelines, workflows, jobs)
        recommendations = self.generate_recommendations(analysis)
        bottlenecks = self.identify_bottlenecks(analysis)
        optimization_plan = self.generate_optimization_plan(bottlenecks, analysis)

        # Save data
        self.save_data(
            output_dir, pipelines, workflows, jobs, analysis, recommendations
        )

        # Create visualizations
        charts_path = os.path.join(output_dir, "performance_charts.png")
        self.create_visualizations(workflows, jobs, charts_path)

        # Save detailed results
        detailed_results = {
            "bottlenecks": bottlenecks,
            "optimization_plan": optimization_plan,
            "analysis_date": datetime.now().isoformat(),
        }

        with open(os.path.join(output_dir, "detailed_analysis.json"), "w") as f:
            json.dump(detailed_results, f, indent=2)

        self.logger.info("✅ Analysis complete!")

        return {
            "summary": analysis["summary"],
            "bottlenecks": bottlenecks,
            "optimization_plan": optimization_plan,
            "output_dir": output_dir,
        }
