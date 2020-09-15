from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

# Defines the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.CreateS3BucketOperator,
        operators.UploadFilesToS3Operator,
        operators.CheckS3FileCount,
        operators.S3ToRedshiftOperator,
        operators.LoadTableOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlCreate,
        helpers.SqlLoad
    ]