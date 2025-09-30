"""
AWS S3 Download Service
Handles downloading files from S3 buckets with pagination and progress tracking.
"""

import os
import boto3  # type: ignore
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, NoCredentialsError  # type: ignore
from utils.logging.logging_manager import LogManager
from utils.file_manager import FileManager


class S3DownloadService:
    """Service for downloading files from S3 buckets."""

    def __init__(self):
        """Initialize the S3 download service."""
        self.logger = LogManager.get_instance().get_logger("S3DownloadService")
        self.s3_client: Optional[Any] = None

    def _initialize_s3_client(self) -> bool:
        """Initialize S3 client with AWS credentials."""
        try:
            self.s3_client = boto3.client("s3")
            # Test credentials by listing buckets
            self.s3_client.list_buckets()
            self.logger.info("Successfully initialized S3 client with AWS credentials")
            return True
        except NoCredentialsError:
            self.logger.error("❌ AWS credentials not found!")
            self.logger.error(
                "Please configure AWS credentials using one of these methods:"
            )
            self.logger.error("1. AWS CLI: run 'aws configure'")
            self.logger.error("2. Environment variables:")
            self.logger.error("   export AWS_ACCESS_KEY_ID=your_access_key")
            self.logger.error("   export AWS_SECRET_ACCESS_KEY=your_secret_key")
            self.logger.error("   export AWS_DEFAULT_REGION=us-east-1")
            self.logger.error("3. AWS credentials file: ~/.aws/credentials")
            return False
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            self.logger.error("❌ AWS access denied or invalid credentials")
            self.logger.error(f"Error details: {error_code} - {e}")
            if error_code == "InvalidAccessKeyId":
                self.logger.error("Your AWS Access Key ID is invalid")
            elif error_code == "SignatureDoesNotMatch":
                self.logger.error("Your AWS Secret Access Key is invalid")
            elif error_code == "AccessDenied":
                self.logger.error(
                    "Your AWS credentials don't have permission to access S3"
                )
            self.logger.error("Please check your AWS credentials and permissions")
            return False
        except Exception as e:
            self.logger.error(f"❌ Unexpected error initializing S3 client: {e}")
            self.logger.error("This might be a network connectivity issue")
            return False

    def list_s3_objects(self, bucket: str, prefix: str) -> List[Dict[str, Any]]:
        """
        List all objects in S3 bucket with given prefix, handling pagination.

        Args:
            bucket: S3 bucket name
            prefix: Object prefix to filter by

        Returns:
            List of S3 object metadata dictionaries
        """
        if self.s3_client is None:
            raise Exception("S3 client not initialized")

        objects = []
        continuation_token = None

        try:
            while True:
                # Build request parameters
                params = {
                    "Bucket": bucket,
                    "Prefix": prefix,
                    "MaxKeys": 1000,  # Maximum allowed by AWS
                }

                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                # Make request
                response = self.s3_client.list_objects_v2(**params)

                # Add objects to list
                if "Contents" in response:
                    objects.extend(response["Contents"])
                    self.logger.info(
                        f"Found {len(response['Contents'])} objects in this page"
                    )

                # Check if there are more objects
                if response.get("IsTruncated", False):
                    continuation_token = response.get("NextContinuationToken")
                    self.logger.info("More objects available, fetching next page...")
                else:
                    break

            self.logger.info(f"Total objects found: {len(objects)}")
            return objects

        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error listing S3 objects: {e}")
            raise

    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        """
        Download a single file from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local file path to save to

        Returns:
            True if successful, False otherwise
        """
        if self.s3_client is None:
            raise Exception("S3 client not initialized")

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # Download file
            self.s3_client.download_file(bucket, key, local_path)
            return True

        except ClientError as e:
            self.logger.error(f"Error downloading {key}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error downloading {key}: {e}")
            return False

    def download_all_files(
        self,
        bucket: str,
        prefix: str,
        local_dir: str = "downloads",
        recursive: bool = True,
        max_files: Optional[int] = None,
        file_extensions: Optional[List[str]] = None,
        skip_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Download all files from S3 bucket with given prefix.

        Args:
            bucket: S3 bucket name
            prefix: Object prefix to filter by
            local_dir: Local directory to download files to
            recursive: If True, download files from subdirectories; if False, only current level
            max_files: Maximum number of files to download (None for unlimited)
            file_extensions: List of file extensions to filter by (e.g., ['.json', '.csv'])
            skip_existing: If True, skip files that already exist with same size

        Returns:
            Dictionary with download statistics
        """
        if not self._initialize_s3_client():
            raise Exception("Failed to initialize S3 client")

        # Create local directory
        FileManager.create_folder(local_dir)
        self.logger.info(f"Created local directory: {local_dir}")

        # List all objects
        self.logger.info(
            f"Listing objects in bucket '{bucket}' with prefix '{prefix}'..."
        )
        if not recursive:
            self.logger.info(
                "Non-recursive mode: will only download files at the current prefix level"
            )
        if max_files:
            self.logger.info(f"Limited to maximum {max_files} files")
        if file_extensions:
            self.logger.info(f"Filtering by extensions: {file_extensions}")

        objects = self.list_s3_objects(bucket, prefix)

        if not objects:
            self.logger.warning("No objects found with the specified prefix")
            return {"total_files": 0, "downloaded": 0, "failed": 0, "skipped": 0}

        # Filter objects based on parameters
        filtered_objects = []
        for obj in objects:
            key = obj["Key"]

            # Skip directories
            if key.endswith("/"):
                continue

            # Apply recursive filter
            if not recursive:
                # Remove prefix and check if there are any remaining slashes
                relative_key = key[len(prefix) :] if key.startswith(prefix) else key
                if relative_key.startswith("/"):
                    relative_key = relative_key[1:]
                if "/" in relative_key:
                    continue  # Skip files in subdirectories

            # Apply file extension filter
            if file_extensions:
                if not any(
                    key.lower().endswith(ext.lower()) for ext in file_extensions
                ):
                    continue

            filtered_objects.append(obj)

            # Apply max files limit
            if max_files and len(filtered_objects) >= max_files:
                break

        self.logger.info(f"After filtering: {len(filtered_objects)} files to download")

        # Download statistics
        stats = {
            "total_files": len(filtered_objects),
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
        }

        # Download each file
        for i, obj in enumerate(filtered_objects, 1):
            key = obj["Key"]
            size = obj["Size"]

            # Create local file path, preserving directory structure
            # Remove the prefix from the key to avoid duplicating it in local path
            relative_key = key
            if key.startswith(prefix):
                relative_key = key[len(prefix) :]
                # Remove leading slash if present
                if relative_key.startswith("/"):
                    relative_key = relative_key[1:]

            local_path = os.path.join(local_dir, relative_key)

            # Check if file already exists
            if skip_existing and os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == size:
                    self.logger.info(
                        f"[{i}/{len(filtered_objects)}] File already exists "
                        f"with same size, skipping: {key}"
                    )
                    stats["skipped"] += 1
                    continue

            # Download file
            self.logger.info(
                f"[{i}/{len(filtered_objects)}] Downloading: {key} ({size:,} bytes)"
            )

            if self.download_file(bucket, key, local_path):
                stats["downloaded"] += 1
                self.logger.info(
                    f"[{i}/{len(filtered_objects)}] ✅ Successfully downloaded: {key}"
                )
            else:
                stats["failed"] += 1
                self.logger.error(
                    f"[{i}/{len(filtered_objects)}] ❌ Failed to download: {key}"
                )

        # Log summary
        self.logger.info("=" * 60)
        self.logger.info("DOWNLOAD SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total files found: {stats['total_files']}")
        self.logger.info(f"Successfully downloaded: {stats['downloaded']}")
        self.logger.info(f"Failed downloads: {stats['failed']}")
        self.logger.info(f"Skipped (existing/directories): {stats['skipped']}")
        self.logger.info(f"Local directory: {os.path.abspath(local_dir)}")

        return stats
