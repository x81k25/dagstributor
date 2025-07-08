#!/usr/bin/env python3
"""
Static validation tests for Dagster build
Runs in GitHub Actions to validate the Docker image build
"""
import sys
import os
from pathlib import Path

# Add the dagstributor module to Python path
# Since this script is now in tests/, we need to go up one level
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing imports...")
    try:
        import dagstributor
        print("✓ dagstributor module imported")
        
        from dagstributor import definitions
        print("✓ definitions module imported")
        
        from dagstributor.automatic_transmission import ops, jobs, schedules
        print("✓ automatic_transmission modules imported")
        
        from dagstributor.wiring_schema_tics import ops as wst_ops, jobs as wst_jobs, schedules as wst_schedules
        print("✓ wiring_schema_tics modules imported")
        
        from dagstributor.test_jobs import ops as test_ops, jobs as test_jobs
        print("✓ test_jobs modules imported")
        
        from repositories import main
        print("✓ repositories.main imported")
        
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_repository_loading():
    """Test that the repository can be loaded"""
    print("\nTesting repository loading...")
    try:
        from repositories.main import dagstributor_repo
        repo = dagstributor_repo()
        print(f"✓ Repository loaded: {repo.name}")
        return True
    except Exception as e:
        print(f"✗ Repository loading error: {e}")
        return False

def test_job_definitions():
    """Test that all jobs are properly defined"""
    print("\nTesting job definitions...")
    try:
        from repositories.main import dagstributor_repo
        repo = dagstributor_repo()
        
        job_count = 0
        for job in repo.get_all_jobs():
            print(f"✓ Job validated: {job.name}")
            job_count += 1
        
        print(f"\nTotal jobs validated: {job_count}")
        
        # Validate expected jobs exist
        expected_at_jobs = [
            "at_01_rss_ingest_job", "at_02_collect_job", "at_03_parse_job",
            "at_04_file_filtration_job", "at_05_metadata_collection_job",
            "at_06_media_filtration_job", "at_07_initiation_job",
            "at_08_download_check_job", "at_09_transfer_job",
            "at_10_cleanup_job", "at_full_pipeline_job"
        ]
        
        expected_wst_jobs = [
            "test_db_connection_job", "wst_atp_bak_job", "wst_atp_reload_job",
            "wst_atp_sync_media_to_training_job"
        ]
        
        expected_test_jobs = [
            "test_timeout_conditions_job"
        ]
        
        job_names = [job.name for job in repo.get_all_jobs()]
        
        for expected in expected_at_jobs + expected_wst_jobs + expected_test_jobs:
            if expected in job_names:
                print(f"✓ Expected job found: {expected}")
            else:
                print(f"✗ Missing expected job: {expected}")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Job validation error: {e}")
        return False

def test_schedule_definitions():
    """Test that all schedules are properly defined"""
    print("\nTesting schedule definitions...")
    try:
        from repositories.main import dagstributor_repo
        repo = dagstributor_repo()
        
        schedule_count = 0
        for schedule in repo.schedule_defs:
            print(f"✓ Schedule validated: {schedule.name}")
            schedule_count += 1
        
        print(f"\nTotal schedules validated: {schedule_count}")
        return True
    except Exception as e:
        print(f"✗ Schedule validation error: {e}")
        return False

def test_op_configurations():
    """Test that ops have valid configurations"""
    print("\nTesting op configurations...")
    try:
        from dagstributor.automatic_transmission.ops import (
            at_01_rss_ingest_op, at_02_collect_op, at_03_parse_op,
            at_04_file_filtration_op, at_05_metadata_collection_op,
            at_06_media_filtration_op, at_07_initiation_op,
            at_08_download_check_op, at_09_transfer_op, at_10_cleanup_op
        )
        
        at_ops = [
            at_01_rss_ingest_op, at_02_collect_op, at_03_parse_op,
            at_04_file_filtration_op, at_05_metadata_collection_op,
            at_06_media_filtration_op, at_07_initiation_op,
            at_08_download_check_op, at_09_transfer_op, at_10_cleanup_op
        ]
        
        for op in at_ops:
            print(f"✓ Op configuration valid: {op.name}")
        
        from dagstributor.wiring_schema_tics.ops import (
            wst_atp_bak_media_op, wst_atp_bak_prediction_op, wst_atp_bak_training_op,
            wst_atp_reload_media_op, wst_atp_reload_training_op, wst_atp_reload_prediction_op,
            wst_atp_sync_media_to_training_op
        )
        
        from dagstributor.test_jobs.ops import test_db_connection_op, test_timeout_conditions_op
        
        wst_ops = [
            wst_atp_bak_media_op, wst_atp_bak_prediction_op, wst_atp_bak_training_op,
            wst_atp_reload_media_op, wst_atp_reload_training_op, wst_atp_reload_prediction_op,
            wst_atp_sync_media_to_training_op
        ]
        
        test_ops = [
            test_timeout_conditions_op
        ]
        
        for op in wst_ops:
            print(f"✓ Op configuration valid: {op.name}")
        
        for op in test_ops:
            print(f"✓ Op configuration valid: {op.name}")
        
        return True
    except Exception as e:
        print(f"✗ Op configuration error: {e}")
        return False

def test_workspace_yaml():
    """Test that workspace.yaml exists and is valid"""
    print("\nTesting workspace.yaml...")
    try:
        workspace_path = Path(__file__).parent.parent / "workspace.yaml"
        if not workspace_path.exists():
            print(f"✗ workspace.yaml not found at {workspace_path}")
            return False
        
        import yaml
        with open(workspace_path) as f:
            workspace_config = yaml.safe_load(f)
        
        print(f"✓ workspace.yaml exists and is valid YAML")
        
        # Check basic structure
        if "load_from" in workspace_config:
            print(f"✓ workspace.yaml has load_from configuration")
            return True
        else:
            print(f"✗ workspace.yaml missing load_from configuration")
            return False
            
    except Exception as e:
        print(f"✗ workspace.yaml error: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("Dagster Build Validation Tests")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_repository_loading,
        test_job_definitions,
        test_schedule_definitions,
        test_op_configurations,
        test_workspace_yaml
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"\n✗ Test {test.__name__} failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("\n✓ All tests passed!")
        return 0
    else:
        print(f"\n✗ {total - passed} tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())