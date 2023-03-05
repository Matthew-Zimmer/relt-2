import axios from 'axios';
import { AlertAdapter } from "..";
import { Project } from '../../../project';

export function webexAlertAdapter(project: Project): AlertAdapter {
  const { webex: { roomId, host } } = project;

  return {
    msg: async (msg) => {
      await axios.post(`${host}/v1/messages`, {
        roomId,
        markdown: msg,
      });
    },
  };
}
