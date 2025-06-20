"""Test using existing at_10_cleanup_op to verify k8s execution"""

from dagster import job
from dagstributor.automatic_transmission.ops import at_10_cleanup_op

@job
def test_existing_k8s_job():
    """Test job using existing k8s op to verify execution works"""
    at_10_cleanup_op()