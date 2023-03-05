import axios from "axios";
import { JobAdapter } from "..";
import { Project } from "../../../project";

export function databricksJobAdapter(project: Project): JobAdapter {
  const { databricks: { host } } = project;

  return {
    create: async (options) => {
      const { name, jarPath } = options;
      const res = await axios.post(`https://${host}/api/2.1/jobs/create`, {
        name,
        tags: {},
        tasks: [
          {
            task_key: "Sessionize",
            description: "Extracts session data from events",
            depends_on: [],
            existing_cluster_id: "0923-164208-meows279",
            spark_jar_task: {
              main_class_name: "com.databricks.Sessionize",
              parameters: [],
            },
            libraries: [{
              jar: jarPath,
            }],
            timeout_seconds: 86400,
            max_retries: 3,
            min_retry_interval_millis: 2000,
            retry_on_timeout: false

          },
        ],
        job_clusters: [{
          job_cluster_key: "auto_scaling_cluster",
          new_cluster: {
            spark_version: "7.3.x-scala2.12",
            node_type_id: "i3.xlarge",
            spark_conf: {
              "spark.speculation": true
            },
            aws_attributes: {
              availability: "SPOT",
              zone_id: "us-west-2a"
            },
            autoscale: {
              min_workers: 2,
              max_workers: 16
            }
          }
        }],
        email_notifications: {
          on_start: [
            "user.name@databricks.com"
          ],
          on_success: [
            "user.name@databricks.com"
          ],
          on_failure: [
            "user.name@databricks.com"
          ],
          no_alert_for_skipped_runs: false
        },
        webhook_notifications: {
          on_start: [{
            id: "03dd86e4-57ef-4818-a950-78e41a1d71ab"
          }],
          on_success: [{
            id: "03dd86e4-57ef-4818-a950-78e41a1d71ab"
          }],
          on_failure: [{
            id: "0481e838-0a59-4eff-9541-a4ca6f149574"
          }]
        },
        timeout_seconds: 86400,
        schedule: {
          quartz_cron_expression: "20 30 * * * ?",
          timezone_id: "Europe/London",
          pause_status: "PAUSED"
        },
        max_concurrent_runs: 10,
        git_source: {
          git_url: "https://github.com/databricks/databricks-cli",
          git_branch: "main",
          git_provider: "gitHub"
        },
        format: "MULTI_TASK",
        access_control_list: [{
          user_name: "jsmith@example.com",
          permission_level: "CAN_MANAGE"
        }]
      });

      return {
        id: res.data.job_id,
      };
    },
    remove: async (options) => {
      const { id } = options;

      await axios.post(`https://${host}/api/2.1/jobs/delete`, {
        job_id: id,
      });
    },
    run: async (options) => {
      const { id, parameters } = options;

      await axios.post(`https://${host}/api/2.1/jobs/run-now`, {
        job_id: id,
        idempotency_token: "TODO",
        jar_params: parameters,
      });
    },
  };
}
