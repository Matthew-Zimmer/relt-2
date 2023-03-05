"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.gitlabRunnerAdapter = void 0;
const promises_1 = require("fs/promises");
function gitlabRunnerAdapter(project) {
    const { name } = project;
    return {
        createConfigs: () => __awaiter(this, void 0, void 0, function* () {
            yield (0, promises_1.writeFile)(`${name}/.gittlab-ci.yml`, `\
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
`);
        })
    };
}
exports.gitlabRunnerAdapter = gitlabRunnerAdapter;
