import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ICommandPalette } from '@jupyterlab/apputils';
import { ISettingRegistry } from '@jupyterlab/settingregistry';

import { requestAPI } from './handler';
import { BatchJobManager } from './batchJobManager';
import '../style/index.css';

/**
 * Initialization data for the myextension extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'myextension:plugin',
  autoStart: true,
  optional: [ICommandPalette, ISettingRegistry],
  activate: (
    app: JupyterFrontEnd,
    palette: ICommandPalette,
    settingRegistry: ISettingRegistry | null
  ) => {
    console.log('JupyterLab extension myextension is activated!');

    const { commands, shell } = app;
    const command = 'widgets:batch-job-manager';

    commands.addCommand(command, {
      label: 'Batch Job Manager',
      caption: 'Manages batch jobs',
      execute: () => {
        const widget = new BatchJobManager();
        shell.add(widget, 'main');
      }
    });
    palette.addItem({ command: command, category: 'Tutorial' });

    if (settingRegistry) {
      settingRegistry
        .load(plugin.id)
        .then(settings => {
          console.log('myextension settings loaded:', settings.composite);
        })
        .catch(reason => {
          console.error('Failed to load settings for myextension.', reason);
        });
    }

    requestAPI<any>('get_example')
      .then(data => {
        console.log(data);
      })
      .catch(reason => {
        console.error(
          `The myextension server extension appears to be missing.\n${reason}`
        );
      });
  }
};

export default plugin;
