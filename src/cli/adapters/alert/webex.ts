// @ts-expect-error
import Webex from 'webex';
import { AlertAdapter } from "..";
import { Project } from '../../../project';

export function webexAlertAdapter(project: Project): AlertAdapter {
  const { webex: { roomId } } = project;

  const webex = Webex.init({
    credentials: {
      access_token: process.env.WEBEX_TOKEN!,
    }
  });

  return {
    msg: async (msg) => {
      webex.messages.create({
        roomId,
        markdown: msg,
      });
    },
  };
}
