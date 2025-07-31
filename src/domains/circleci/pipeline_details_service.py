"""
CircleCI Pipeline Details Service
Service for extracting detailed information from a specific CircleCI pipeline
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional

from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager


class PipelineDetailsService:
    """Service for extracting detailed information from a specific CircleCI pipeline"""
    
    def __init__(self, token: str, project_slug: str):
        """
        Initialize CircleCI Pipeline Details service
        
        Args:
            token: CircleCI API token
            project_slug: Project slug (e.g., 'gh/organization/repository-name')
        """
        self.token = token
        self.project_slug = project_slug
        self.base_url = "https://circleci.com/api/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Circle-Token': token,
            'Accept': 'application/json'
        })
        self.logger = LogManager.get_instance().get_logger("PipelineDetailsService")
        self.cache = CacheManager.get_instance()
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request to CircleCI with error handling"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            self.logger.debug(f"Making request to: {url}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"Failed to make request to {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response body: {e.response.text}")
            raise
    
    def get_pipeline_by_number(self, pipeline_number: int) -> Optional[Dict]:
        """Get pipeline by number using the project pipelines endpoint"""
        self.logger.info(f"🔍 Searching for pipeline number {pipeline_number}")
        
        try:
            # Get recent pipelines for the project
            endpoint = f"/project/{self.project_slug}/pipeline"
            params = {"page-token": None}
            
            # Search through multiple pages if needed
            for page in range(5):  # Limit to 5 pages to avoid infinite loop
                data = self._make_request(endpoint, params)
                
                for pipeline in data.get('items', []):
                    if pipeline.get('number') == pipeline_number:
                        self.logger.info(f"✅ Found pipeline {pipeline_number}: {pipeline.get('id')}")
                        return pipeline
                
                # Check for next page
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    break
                params["page-token"] = next_page_token
            
            self.logger.warning(f"⚠️  Pipeline number {pipeline_number} not found in recent pipelines")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Error searching for pipeline {pipeline_number}: {e}")
            return None
    
    def get_pipeline_details(self, pipeline_id: Optional[str] = None, 
                           pipeline_number: Optional[int] = None,
                           verbose: bool = False) -> Optional[Dict]:
        """
        Get comprehensive details for a specific pipeline
        
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
                pipeline_id = pipeline.get('id')
            
            if not pipeline_id:
                self.logger.error("❌ Pipeline ID is required")
                return None
            
            self.logger.info(f"📊 Extracting details for pipeline: {pipeline_id}")
            
            # Get base pipeline information
            pipeline_data = self._make_request(f"/pipeline/{pipeline_id}")
            
            # Enhance with additional details
            detailed_pipeline = {
                'pipeline_info': pipeline_data,
                'workflows': self._get_pipeline_workflows(pipeline_id, verbose),
                'metadata': self._get_pipeline_metadata(pipeline_data),
                'analysis_timestamp': datetime.now().isoformat(),
                'project_slug': self.project_slug
            }
            
            # Add trigger analysis
            detailed_pipeline['trigger_analysis'] = self._analyze_trigger(pipeline_data)
            
            self.logger.info("✅ Pipeline details extracted successfully")
            return detailed_pipeline
            
        except Exception as e:
            self.logger.error(f"❌ Error getting pipeline details: {e}")
            return None
    
    def _get_pipeline_workflows(self, pipeline_id: str, verbose: bool = False) -> List[Dict]:
        """Get all workflows for a pipeline with detailed information"""
        try:
            self.logger.info(f"🔄 Getting workflows for pipeline: {pipeline_id}")
            
            workflow_data = self._make_request(f"/pipeline/{pipeline_id}/workflow")
            workflows = []
            
            for workflow in workflow_data.get('items', []):
                workflow_detail = {
                    'id': workflow.get('id'),
                    'name': workflow.get('name'),
                    'status': workflow.get('status'),
                    'created_at': workflow.get('created_at'),
                    'stopped_at': workflow.get('stopped_at'),
                    'project_slug': workflow.get('project_slug'),
                    'tag': workflow.get('tag')
                }
                
                # Calculate duration
                if workflow.get('stopped_at') and workflow.get('created_at'):
                    start = datetime.fromisoformat(workflow['created_at'].replace('Z', '+00:00'))
                    stop = datetime.fromisoformat(workflow['stopped_at'].replace('Z', '+00:00'))
                    workflow_detail['duration_seconds'] = int((stop - start).total_seconds())
                
                # Get jobs for this workflow
                workflow_detail['jobs'] = self._get_workflow_jobs(workflow['id'], verbose)
                
                workflows.append(workflow_detail)
                
                # Rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"✅ Found {len(workflows)} workflows")
            return workflows
            
        except Exception as e:
            self.logger.error(f"❌ Error getting workflows: {e}")
            return []
    
    def _get_workflow_jobs(self, workflow_id: str, verbose: bool = False) -> List[Dict]:
        """Get all jobs for a workflow with detailed information"""
        try:
            job_data = self._make_request(f"/workflow/{workflow_id}/job")
            jobs = []
            
            for job in job_data.get('items', []):
                job_detail = {
                    'id': job.get('id'),
                    'name': job.get('name'),
                    'status': job.get('status'),
                    'job_number': job.get('job_number'),
                    'type': job.get('type'),
                    'started_at': job.get('started_at'),
                    'stopped_at': job.get('stopped_at')
                }
                
                # Calculate duration
                if job.get('stopped_at') and job.get('started_at'):
                    start = datetime.fromisoformat(job['started_at'].replace('Z', '+00:00'))
                    stop = datetime.fromisoformat(job['stopped_at'].replace('Z', '+00:00'))
                    job_detail['duration_seconds'] = int((stop - start).total_seconds())
                
                # Get detailed job information if verbose
                if verbose:
                    try:
                        job_detail['details'] = self._get_job_details(job['job_number'])
                    except Exception as e:
                        self.logger.warning(f"⚠️  Could not get details for job {job['job_number']}: {e}")
                        job_detail['details'] = {}
                
                jobs.append(job_detail)
                
                # Rate limiting
                time.sleep(0.05)
            
            return jobs
            
        except Exception as e:
            self.logger.error(f"❌ Error getting jobs for workflow {workflow_id}: {e}")
            return []
    
    def _get_job_details(self, job_number: int) -> Dict:
        """Get detailed job information including steps and logs"""
        try:
            job_detail = self._make_request(f"/project/{self.project_slug}/job/{job_number}")
            
            # Simplify the response to include key information
            return {
                'executor': job_detail.get('executor'),
                'parallelism': job_detail.get('parallelism'),
                'resource_class': job_detail.get('resource_class'),
                'web_url': job_detail.get('web_url'),
                'steps_count': len(job_detail.get('steps', [])),
                'has_artifacts': len(job_detail.get('artifacts', [])) > 0,
                'latest_workflow': job_detail.get('latest_workflow', {}).get('name'),
                'contexts': job_detail.get('contexts', [])
            }
            
        except Exception as e:
            self.logger.warning(f"⚠️  Could not get details for job {job_number}: {e}")
            return {}
    
    def _get_pipeline_metadata(self, pipeline_data: Dict) -> Dict:
        """Extract and organize pipeline metadata"""
        vcs_info = pipeline_data.get('vcs', {})
        trigger_info = pipeline_data.get('trigger', {})
        
        return {
            'pipeline_number': pipeline_data.get('number'),
            'created_at': pipeline_data.get('created_at'),
            'updated_at': pipeline_data.get('updated_at'),
            'state': pipeline_data.get('state'),
            'git_info': {
                'revision': vcs_info.get('revision'),
                'branch': vcs_info.get('branch'),
                'tag': vcs_info.get('tag'),
                'commit_url': vcs_info.get('commit', {}).get('url'),
                'commit_subject': vcs_info.get('commit', {}).get('subject'),
                'commit_author': vcs_info.get('commit', {}).get('author', {}).get('name')
            },
            'trigger_info': trigger_info
        }
    
    def _analyze_trigger(self, pipeline_data: Dict) -> Dict:
        """Analyze what triggered the pipeline"""
        trigger = pipeline_data.get('trigger', {})
        vcs = pipeline_data.get('vcs', {})
        
        analysis = {
            'trigger_type': trigger.get('type'),
            'trigger_received_at': trigger.get('received_at'),
            'is_scheduled': trigger.get('type') == 'scheduled_pipeline',
            'is_api': trigger.get('type') == 'api',
            'is_webhook': trigger.get('type') == 'webhook',
            'branch': vcs.get('branch'),
            'tag': vcs.get('tag'),
            'is_tag_trigger': bool(vcs.get('tag')),
            'is_main_branch': vcs.get('branch') in ['main', 'master', 'develop'],
            'commit_message': vcs.get('commit', {}).get('subject', ''),
            'actor': trigger.get('actor', {}).get('login') or trigger.get('actor', {}).get('name')
        }
        
        # Determine likely trigger reason
        if analysis['is_tag_trigger']:
            analysis['likely_reason'] = f"Tag release: {vcs.get('tag')}"
        elif analysis['trigger_type'] == 'webhook' and analysis['branch']:
            analysis['likely_reason'] = f"Push to branch: {analysis['branch']}"
        elif analysis['trigger_type'] == 'api':
            analysis['likely_reason'] = "Manual API trigger or scheduled pipeline"
        else:
            analysis['likely_reason'] = f"Unknown trigger type: {analysis['trigger_type']}"
        
        return analysis
    
    def analyze_pipeline_config(self, pipeline_data: Dict) -> Dict:
        """Analyze the CircleCI configuration for this pipeline"""
        try:
            # This would require access to the .circleci/config.yml file
            # For now, we'll analyze based on the workflow patterns we see
            workflows = pipeline_data.get('workflows', [])
            
            analysis = {
                'total_workflows': len(workflows),
                'workflow_names': [w.get('name') for w in workflows],
                'workflow_statuses': {w.get('name'): w.get('status') for w in workflows},
                'triggered_workflows': [w.get('name') for w in workflows if w.get('status') != 'not_run'],
                'failed_workflows': [w.get('name') for w in workflows if w.get('status') == 'failed'],
                'successful_workflows': [w.get('name') for w in workflows if w.get('status') == 'success']
            }
            
            # Analyze job patterns
            all_jobs = []
            for workflow in workflows:
                for job in workflow.get('jobs', []):
                    all_jobs.append({
                        'workflow': workflow.get('name'),
                        'job_name': job.get('name'),
                        'status': job.get('status'),
                        'type': job.get('type')
                    })
            
            analysis['job_analysis'] = {
                'total_jobs': len(all_jobs),
                'jobs_by_workflow': {w.get('name'): len(w.get('jobs', [])) for w in workflows},
                'job_statuses': {job['job_name']: job['status'] for job in all_jobs},
                'failed_jobs': [job['job_name'] for job in all_jobs if job['status'] == 'failed']
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"❌ Error analyzing pipeline configuration: {e}")
            return {}
    
    def save_to_file(self, data: Dict, file_path: str):
        """Save pipeline data to a JSON file"""
        try:
            JSONManager.write_json(data, file_path)
            self.logger.info(f"✅ Data saved to: {file_path}")
        except Exception as e:
            self.logger.error(f"❌ Error saving to file: {e}")
            raise
    
    def print_pipeline_summary(self, pipeline_data: Dict, verbose: bool = False):
        """Print a formatted summary of the pipeline data"""
        pipeline_info = pipeline_data.get('pipeline_info', {})
        metadata = pipeline_data.get('metadata', {})
        workflows = pipeline_data.get('workflows', [])
        trigger_analysis = pipeline_data.get('trigger_analysis', {})
        
        print("\n" + "="*80)
        print("📊 CIRCLECI PIPELINE ANALYSIS")
        print("="*80)
        
        # Basic Information
        print("\n🔍 PIPELINE INFORMATION:")
        print(f"   Pipeline Number: {metadata.get('pipeline_number')}")
        print(f"   Pipeline ID: {pipeline_info.get('id')}")
        print(f"   Project: {self.project_slug}")
        print(f"   State: {metadata.get('state')}")
        print(f"   Created: {metadata.get('created_at')}")
        
        # Git Information
        git_info = metadata.get('git_info', {})
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
                    'success': '✅',
                    'failed': '❌',
                    'running': '🔄',
                    'canceled': '⏹️',
                    'not_run': '⏸️',
                    'failing': '🔶',
                    'on_hold': '⏸️'
                }.get(workflow.get('status'), '❓')
                
                print(f"\n   📋 WORKFLOW {i}: {workflow.get('name')}")
                print(f"      Status: {status_emoji} {workflow.get('status')}")
                print(f"      ID: {workflow.get('id')}")
                print(f"      Created: {workflow.get('created_at')}")
                print(f"      Stopped: {workflow.get('stopped_at') or 'N/A'}")
                
                if workflow.get('duration_seconds'):
                    duration = workflow['duration_seconds']
                    print(f"      Duration: {duration//60}m {duration%60}s")
                
                # Tag information if available
                if workflow.get('tag'):
                    print(f"      Tag: {workflow.get('tag')}")
                
                # Jobs Analysis
                jobs = workflow.get('jobs', [])
                print(f"      Jobs ({len(jobs)}):")
                
                if not jobs:
                    print("        ⚠️  No jobs found (workflow may have been canceled before jobs started)")
                else:
                    for j, job in enumerate(jobs, 1):
                        try:
                            job_emoji = {
                                'success': '✅',
                                'failed': '❌',
                                'running': '🔄',
                                'canceled': '⏹️',
                                'not_run': '⏸️',
                                'blocked': '🚫',
                                'infrastructure_fail': '🔥'
                            }.get(job.get('status'), '❓')
                            
                            job_duration = ""
                            if job.get('duration_seconds'):
                                d = job['duration_seconds']
                                job_duration = f" ({d//60}m {d%60}s)"
                            
                            print(f"        {job_emoji} {job.get('name')}: {job.get('status')}{job_duration}")
                            print(f"           Job Number: {job.get('job_number')}")
                            print(f"           Type: {job.get('type', 'build')}")
                            
                            if job.get('started_at'):
                                print(f"           Started: {job.get('started_at')}")
                            if job.get('stopped_at'):
                                print(f"           Stopped: {job.get('stopped_at')}")
                            
                            # Show detailed job info if verbose
                            if verbose and job.get('details'):
                                details = job['details']
                                print("           Details:")
                                if details.get('executor'):
                                    print(f"             Executor: {details['executor']}")
                                if details.get('resource_class'):
                                    print(f"             Resource Class: {details['resource_class']}")
                                if details.get('parallelism'):
                                    print(f"             Parallelism: {details['parallelism']}")
                                if details.get('steps_count'):
                                    print(f"             Steps: {details['steps_count']}")
                                if details.get('has_artifacts'):
                                    print(f"             Has Artifacts: {details['has_artifacts']}")
                                if details.get('contexts'):
                                    # Handle contexts properly - they might be dicts or strings
                                    contexts = details['contexts']
                                    if isinstance(contexts, list):
                                        context_names = []
                                        for ctx in contexts:
                                            if isinstance(ctx, dict):
                                                context_names.append(ctx.get('name', str(ctx)))
                                            else:
                                                context_names.append(str(ctx))
                                        print(f"             Contexts: {', '.join(context_names)}")
                                    else:
                                        print(f"             Contexts: {contexts}")
                                if details.get('web_url'):
                                    print(f"             URL: {details['web_url']}")
                        except Exception as e:
                            print(f"        ❌ Error displaying job {j}: {e}")
                            self.logger.error(f"Error displaying job {j}: {e}")
                            
            except Exception as e:
                print(f"   ❌ Error displaying workflow {i}: {e}")
                self.logger.error(f"Error displaying workflow {i}: {e}")
        
        # Configuration Analysis
        config_analysis = pipeline_data.get('config_analysis', {})
        if config_analysis:
            print("\n🔧 CONFIGURATION ANALYSIS:")
            print(f"   Triggered Workflows: {', '.join(config_analysis.get('triggered_workflows', []))}")
            print(f"   Not Triggered: {', '.join(config_analysis.get('workflow_names', [])) if set(config_analysis.get('workflow_names', [])) - set(config_analysis.get('triggered_workflows', [])) else 'None'}")
            print(f"   Failed Workflows: {', '.join(config_analysis.get('failed_workflows', []))} {'' if config_analysis.get('failed_workflows') else '(None)'}")
            print(f"   Successful Workflows: {', '.join(config_analysis.get('successful_workflows', []))} {'' if config_analysis.get('successful_workflows') else '(None)'}")
            print(f"   Total Jobs: {config_analysis.get('job_analysis', {}).get('total_jobs', 0)}")
            
            # Jobs by workflow breakdown
            jobs_by_workflow = config_analysis.get('job_analysis', {}).get('jobs_by_workflow', {})
            if jobs_by_workflow:
                print("   Jobs per Workflow:")
                for wf_name, job_count in jobs_by_workflow.items():
                    print(f"     - {wf_name}: {job_count} jobs")
            
            # Failed jobs
            failed_jobs = config_analysis.get('job_analysis', {}).get('failed_jobs', [])
            if failed_jobs:
                print(f"   Failed Jobs: {', '.join(failed_jobs)}")
        
        # Trigger Analysis Details
        print("\n🎯 TRIGGER PATTERN ANALYSIS:")
        tag = git_info.get('tag')
        branch = git_info.get('branch')
        
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
        
        print("\n" + "="*80)
