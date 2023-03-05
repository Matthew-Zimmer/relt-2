import { RunnerAdapter } from "..";

export function gitlabRunnerAdapter(): RunnerAdapter {
  return [[".gitlab-ci.yml",
    `
stages:
  - deploy

image: relt:latest

deploy preview:
  stage: deploy
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_submit"'
  script: |
    relt deploy $branch --using aws databricks webex

redeploy preview:
  stage: deploy
  rules:
    - if: '$CI_COMMIT_REF_NAME != "master" && $CI_PIPELINE_SOURCE == "push"'
  script: |
    relt redeploy $branch --using aws databricks webex

destroy preview:
  stage: deploy
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_closed"'
  script: |
    relt destroy $branch --using aws databricks webex

deploy prod check:
  stage: deploy
  rules:
    - if: '$CI_COMMIT_REF_NAME == "master" && $CI_PIPELINE_SOURCE == "push"'
  script: |
    relt redeploy $branch --using aws databricks webex

deploy prod:
  stage: deploy
  rules:
    - if: '$CI_COMMIT_REF_NAME == "master" && $CI_PIPELINE_SOURCE == "tag"'
  script: |
    relt redeploy $branch --using aws databricks webex
`
  ]]
}