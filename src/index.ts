import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ICommandPalette, MainAreaWidget } from '@jupyterlab/apputils';
import { IFileBrowserFactory } from '@jupyterlab/filebrowser';
import { ILauncher } from '@jupyterlab/launcher';
import { ISettingRegistry } from '@jupyterlab/settingregistry';

import { requestAPI } from './handler';
import { batchManagerIcon } from './icons';
import { BatchJobManager } from './batchJobManager';
import 'bootstrap/dist/js/bootstrap.bundle.min.js';
import '../style/index.css';

/**
 * Initialization data for the myextension extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'myextension:plugin',
  autoStart: true,
  requires: [IFileBrowserFactory, ILauncher, ICommandPalette],
  optional: [ISettingRegistry],
  activate: (
    app: JupyterFrontEnd,
    factory: IFileBrowserFactory,
    launcher: ILauncher,
    palette: ICommandPalette,
    settingRegistry: ISettingRegistry | null
  ) => {
    console.log('JupyterLab extension myextension is activated!');

    let widget: MainAreaWidget<BatchJobManager>;

    const command = 'batch-job-manager:open';
    app.commands.addCommand(command, {
      label: 'Batch Job Manager',
      caption: 'Manages batch jobs',
      icon: batchManagerIcon,
      execute: () => {
        if (!widget || widget.isDisposed) {
          console.log('Filling batch-job widget!');
          const content = new BatchJobManager(factory, app.commands);
          widget = new MainAreaWidget({ content });
          widget.id = 'batch-job-manager';
        }

        if (!widget.isAttached) {
          console.log('Attaching batch-job widget!');
          // Attach the widget to the main work area if it's not there
          app.shell.add(widget, 'main');
        }
        widget.content.update();

        app.shell.activateById(widget.id);
      }
    });
    palette.addItem({ command: command, category: 'Tutorial' });
    launcher.add({
      command,
      category: 'Other',
      rank: 6
    });
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

    requestAPI<any>('testhub')
      .then(data => {
        console.log(data);
      })
      .catch(reason => {
        console.error(
          `Looks like a connection error: Jupyter Server --> JupyterHub service.\n${reason}`
        );
      });

    requestAPI<any>('settings')
      .then(data => {
        console.debug(data);
      })
      .catch(reason => {
        console.error(
          `Failed to get settings available to handlers.\n${reason}`
        );
      });

    requestAPI<any>('config')
      .then(data => {
        console.debug(data);
      })
      .catch(reason => {
        console.error(
          `Failed to get settings available to handlers.\n${reason}`
        );
      });
  }
};

export default plugin;
