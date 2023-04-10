import { Dialog, showDialog, Notification } from '@jupyterlab/apputils';
import { FileDialog, IFileBrowserFactory } from '@jupyterlab/filebrowser';
import { Widget } from '@lumino/widgets';
import { ServerConnection } from '@jupyterlab/services';
import { URLExt } from '@jupyterlab/coreutils';
import { CommandRegistry } from '@lumino/commands';

import { IBatchJobItem } from './types';
import {
  escapeHtmlAttribute,
  getOutputPath,
  fileExists,
  toOptionTags
} from './utils';

const SERVER_URL = '/myextension';

const JOB_TABLE = `
<div class="container mt-5 job-table-page">
  <div class="container job-table-alert">
  </div>
  <div class="container job-table-header d-flex align-items-center">
    <button type="button" class="btn btn-secondary my-update-button">
    <i class="bi bi-arrow-clockwise"></i> Reload
    </button>
  </div>
  <table class="table table-striped">
    <thead>
      <tr>
        <th scope="col">Job ID</th>
        <th scope="col">Name</th>
        <th scope="col">Input File</th>
        <th scope="col">Outputs</th>
        <th scope="col">Created At</th>
        <th scope="col">Instance Type</th>
        <th scope="col">Status</th>
        <th scope="col">Actions</th>
        <th class="text-center">
          <button type="button" class="btn btn-primary my-create-button">
            Create Job
          </button>
        </th>
      </tr>
    </thead>
    <tbody id="jobs-table-body">
    </tbody>
  </table>
</div>
`;

const INSTANCE_TYPES = [
  't3.medium',
  't3a.medium',
  'm6a.large',
  'm6a.xlarge',
  'm6a.2xlarge',
  'm6a.4xlarge',
  'm6a.8xlarge',
  'm6a.12xlarge',
  // 'm6a.16xlarge',
  // 'm6a.24xlarge',
  // 'm6a.32large',
  // 'm6a.48xlarge',
  // 'm6a.metal',
  // ------------------------
  'c6a.large',
  'c6a.xlarge',
  'c6a.2xlarge',
  'c6a.4xlarge',
  'c6a.8xlarge',
  'c6a.12xlarge',
  // 'c6a.16xlarge',
  // 'c6a.24xlarge',
  // 'c6a.32large',
  // 'c6a.48xlarge',
  // 'c6a.metal',
  //
  'r6a.large',
  'r6a.xlarge',
  'r6a.2xlarge',
  'r6a.4xlarge',
  'r6a.8xlarge',
  'r6a.12xlarge',
  // 'r6a.16xlarge',
  // 'r6a.24xlarge',
  'r6a.32large'
  // 'r6a.48xlarge',
  // 'r6a.metal',
];

const JOB_DEFINITION = `
<form id="create-job-form">
  <div class="mb-3">
    <label for="job-name" class="form-label">Name</label>
    <input type="text" class="form-control" id="job-name" placeholder="Enter job name">
  </div>
  <div class="mb-3">
    <label for="job-file-path" class="form-label">File Path</label>
    <input type="text" class="form-control" id="job-file-path" placeholder="Enter file path">
    <button type="button" id="job-file-path-button" class="btn btn-secondary">Browse</button>
  </div>
  <div class="mb-3">
    <label for="job-instance-type" class="form-label">Instance Type (<a href="https://aws.amazon.com/ec2/instance-types/" target="_blank">INFO</a>)</label>
    <select class="form-select" id="job-instance-type">
      ${toOptionTags(INSTANCE_TYPES)}
    </select>
  </div>
  <div class="mb-3">
    <label for="job-max-coins-per-hour" class="form-label">Max Coins Per Hour: <span id="job-max-coins-per-hour-value">10</span></label>
    <input type="range" min="5" max="100" value="10" step="1" class="form-range" id="job-max-coins-per-hour">
  </div>
</form>
`;

export class BatchJobManager extends Widget {
  constructor(
    private factory: IFileBrowserFactory,
    private commands: CommandRegistry
  ) {
    super();
    console.log('BatchJobManager: constructor()!');
    this.node.innerHTML = JOB_TABLE;
    this.title.label = 'Batch Jobs';
    this.title.closable = true;
    // https://jupyterlab.readthedocs.io/en/stable/developer/css.html
    this.addClass('jp-BatchJobManager');

    this.node
      .querySelector('.my-create-button')
      ?.addEventListener('click', () => {
        this.showCreateJobDialog();
      });

    this.node
      .querySelector('.my-update-button')
      ?.addEventListener('click', async () => {
        this.showSpinner();
        await this.fetchJobs();
        this.removeSpinner();
      });
  }

  async onAfterAttach(): Promise<void> {
    console.log('BatchJobManager: onAfterAttach()!');
    this.showSpinner();
    await this.fetchJobs();
    this.removeSpinner();
  }

  // Use JupyterLab API instead of fetch() to make the auth easy
  private fetch(endPoint: string, init: RequestInit = {}) {
    const settings = ServerConnection.makeSettings();
    const requestUrl = URLExt.join(settings.baseUrl, SERVER_URL, endPoint);
    return ServerConnection.makeRequest(requestUrl, init, settings);
  }

  async fetchJobs(): Promise<void> {
    console.log('BatchJobManager: fetchJobs()!');
    let response: Response;
    try {
      response = await this.fetch('/jobs');
    } catch (error) {
      throw new ServerConnection.NetworkError(error as TypeError);
    }
    if (!response.ok) {
      throw new ServerConnection.ResponseError(response);
    }

    const jobs = (await response.json()) as IBatchJobItem[];
    const tableBody = this.node.querySelector(
      '#jobs-table-body'
    ) as HTMLElement;
    tableBody.innerHTML = '';

    for (const job of jobs) {
      const row = document.createElement('tr');
      row.id = `job-table-row-${job.job_id}`;
      const escaped_console = escapeHtmlAttribute(job.console_output);
      const outputPath = getOutputPath(job);
      row.innerHTML = `
        <td>${job.job_id}</td>
        <td>${job.name}</td>
        <td><a href="#" class="job-table-row-input-link"></a></td>
        <td class="job-table-row-output">
          <a href="#" class="job-table-row-output-link"></a>
        </td>
        <td>${job.timestamp}</td>
        <td>${job.instance_type}</td>
        <td><a href="#" class="job-status" data-job-log="${escaped_console}">${job.status}</a></td>
        <td><button class="btn btn-danger btn-sm delete-job" data-job-id="${job.job_id}">Delete</button></td>
      `;

      // Add link to input file
      this.addlink(row, 'job-table-row-input-link', job.file_path);

      // Add nothing / button to download / link to output file
      if (await fileExists(outputPath)) {
        this.addlink(row, 'job-table-row-output-link', outputPath);
      } else if (job.status === 'TERMINATED' || job.status === 'EMPTY') {
        console.log('-----------------------------------');
        console.log('Request downloading from B2!');
        console.log('-----------------------------------');
        this.fetch(`/download/${job.job_id}`);
        this.addlink(row, 'job-table-row-output-link', outputPath);
      }

      tableBody.appendChild(row);
    }

    const deleteButtons = this.node.querySelectorAll('.delete-job');
    const clickedButtons = new Set();

    deleteButtons.forEach(button => {
      button.addEventListener('click', async event => {
        event.preventDefault();

        if (!clickedButtons.has(button)) {
          // Change the appearance of the button on the first click
          clickedButtons.add(button);
          button.classList.add('btn-warning');
          button.classList.remove('btn-danger');
          button.textContent = 'Confirm Delete';

          // Reset the button state after a timeout (e.g., 3 seconds)
          setTimeout(() => {
            clickedButtons.delete(button);
            button.classList.add('btn-danger');
            button.classList.remove('btn-warning');
            button.textContent = 'Delete';
          }, 3000);
        } else {
          const job_id = (event.target as HTMLElement).dataset.jobId;
          if (job_id) {
            try {
              this.showAlert('Deleting...', 'deleting-job-alert', 'info');
              await this.deleteJob(job_id);
              await this.fetchJobs();
              this.removeAlert('deleting-job-alert');
            } catch (error) {
              let msg: string;
              if (error instanceof ServerConnection.ResponseError) {
                const json = await error.response.json();
                const cause = json.data;
                msg = `Failed: ${cause}.`;
              } else if (error instanceof ServerConnection.NetworkError) {
                msg = `Failed to delete Job (NetworkError): ${error.message}`;
              } else {
                msg = `Failed to delete Job: ${error}`;
              }
              this.showAlert(msg, 'deleting-job-error-alert', 'danger');
              Notification.error(msg);
            }
          } else {
            console.warn('Job ID is not available');
          }
        }
      });
    });

    const statusLinks = this.node.querySelectorAll('.job-status');
    statusLinks.forEach(link => {
      link.addEventListener('click', event => {
        event.preventDefault();
        const logContent = (event.target as HTMLElement).dataset.jobLog;
        if (logContent) {
          this.showConsoleLog(logContent);
        } else {
          console.warn('Log content is not available');
        }
      });
    });
  }

  private addlink(row: HTMLTableRowElement, classname: string, path: string) {
    const link = row.querySelector(`.${classname}`);
    if (link) {
      link.textContent = path;
      link.addEventListener('click', event => {
        event.preventDefault();
        this.commands.execute('docmanager:open', { path });
      });
    }
  }

  async deleteJob(jobId: string): Promise<void> {
    let response: Response;
    try {
      response = await this.fetch(`/jobs/${jobId}`, {
        method: 'DELETE'
      });
    } catch (error) {
      throw new ServerConnection.NetworkError(error as TypeError);
    }

    if (!response.ok) {
      console.error('Error deleting a job.');
      throw new ServerConnection.ResponseError(response);
    }
  }

  async addJob(
    name: string,
    filePath: string,
    instanceType: string,
    maxCoinsPerHour: string
  ): Promise<void> {
    let response: Response;
    try {
      response = await this.fetch('/jobs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          name: name,
          path: filePath,
          instance_type: instanceType,
          max_coins_per_hour: maxCoinsPerHour
        })
      });
    } catch (error) {
      throw new ServerConnection.NetworkError(error as TypeError);
    }

    if (!response.ok) {
      console.error('Error adding a job.');
      throw new ServerConnection.ResponseError(response);
    }
  }

  private async showCreateJobDialog(): Promise<void> {
    const body = new Widget();
    body.node.innerHTML = JOB_DEFINITION;
    const rangeInput = (body.node as HTMLElement).querySelector(
      '#job-max-coins-per-hour'
    );
    const rangeValue = body.node.querySelector('#job-max-coins-per-hour-value');

    if (rangeInput && rangeValue) {
      rangeInput.addEventListener('input', event => {
        const x = (event.target as HTMLInputElement).value;
        rangeValue.textContent = x;
      });
    }

    const nameInput = body.node.querySelector('#job-name') as HTMLInputElement;
    const filePathInput = body.node.querySelector(
      '#job-file-path'
    ) as HTMLInputElement;
    const filePathButton = body.node.querySelector(
      '#job-file-path-button'
    ) as HTMLButtonElement;

    const options = {
      title: 'Create Job',
      body,
      buttons: [Dialog.cancelButton(), Dialog.okButton({ label: 'Create Job' })]
    };
    let dialog = new Dialog(options);

    // 1. Job definition dialog hides the file-browser dialog.
    // 2. Can avoid overlapping only if the first dialog promise is fulfilled.
    // 3. Main thread need to get values after the last promise is fullfilled.
    //
    // This is why I created a queue; `await` all of its elements.
    //
    const queue: Promise<any>[] = [];
    queue.push(dialog.launch());

    filePathButton.addEventListener('click', async () => {
      const pOpenFiles = FileDialog.getOpenFiles({
        manager: this.factory.defaultBrowser.model.manager
      });

      // Put next promise in the queue before rejecting current one
      queue.push(pOpenFiles);
      dialog.reject();

      const res = await pOpenFiles;
      if (res.button.accept && res.value && res.value.length > 0) {
        nameInput.value = res.value[0].name;
        filePathInput.value = res.value[0].path;
      }

      dialog = new Dialog(options);
      queue.push(dialog.launch());
    });

    let valuesEntered = false;
    while (queue.length > 0) {
      const result = await queue.pop();
      if (
        result.button &&
        result.button.label === 'Create Job' &&
        result.button.accept
      ) {
        valuesEntered = true;
        console.log('result: ', result);
      }
    }

    if (valuesEntered) {
      const name = (body.node.querySelector('#job-name') as HTMLSelectElement)
        .value;
      const filePath = (
        body.node.querySelector('#job-file-path') as HTMLSelectElement
      ).value;
      const instanceType = (
        body.node.querySelector('#job-instance-type') as HTMLSelectElement
      ).value;
      const maxCoinsPerHour = (
        body.node.querySelector('#job-max-coins-per-hour') as HTMLSelectElement
      ).value;
      console.log('Name:', name);
      console.log('File Path:', filePath);
      console.log('Instance Type:', instanceType);
      console.log('maxCoinsPerHour:', maxCoinsPerHour);

      try {
        this.showAlert('Submitting a job...', 'submitting-job-alert', 'info');
        await this.addJob(name, filePath, instanceType, maxCoinsPerHour);
        await this.fetchJobs();
        this.removeAlert('submitting-job-alert');
      } catch (error) {
        let msg: string;
        if (error instanceof ServerConnection.ResponseError) {
          const json = await error.response.json();
          const cause = json.data;
          if (cause === 'SpotMaxPriceTooLow') {
            msg = 'Failed: Increase Max Coins Per Hour in the job definition.';
          } else if (cause === 'InsufficientInstanceCapacity') {
            msg =
              'Failed: Insufficient instance capacity. Wait or choose other instance types.';
          } else {
            msg = `Failed: ${cause}.`;
          }
        } else if (error instanceof ServerConnection.NetworkError) {
          msg = `Failed to Create Job (NetworkError): ${error.message}`;
        } else {
          msg = `Failed to Create Job: ${error}`;
        }
        this.showAlert(msg, 'job-submission-error-alert', 'danger');
        Notification.error(msg);
      }
    }
  }

  private async showConsoleLog(logContent: string): Promise<void> {
    const body = new Widget();
    //
    // TODO: Make the window larger. Make the display terminal-like.
    body.node.innerHTML = `
    <div class="container mt-5"><pre>${logContent}</pre></div>
    `;
    await showDialog({
      title: 'Console Log',
      body,
      buttons: [Dialog.okButton()]
    });
  }

  private async showAlert(
    message: string,
    classname: string,
    type = 'info'
  ): Promise<void> {
    console.info(`showAlert (${type}): ${message}`);

    const alertElement = document.createElement('div');
    alertElement.className = `alert alert-${type} ${classname}`;
    alertElement.setAttribute('role', 'alert');
    alertElement.textContent = message;

    this.node.querySelector('.job-table-alert')?.appendChild(alertElement);

    setTimeout(() => {
      alertElement.remove();
    }, 20000);
  }

  private async removeAlert(classname: string): Promise<void> {
    this.node.querySelector(`.alert.${classname}`)?.remove();
  }

  private async showSpinner(classname = 'my-loading-spinner'): Promise<void> {
    const spinnerElement = document.createElement('div');
    spinnerElement.className = `spinner-border text-secondary ms-2 ${classname}`;
    spinnerElement.setAttribute('role', 'status');

    const child = document.createElement('span');
    child.className = 'visually-hidden';
    child.textContent = 'Loading...';
    spinnerElement.appendChild(child);

    this.node.querySelector('.job-table-header')?.appendChild(spinnerElement);

    setTimeout(() => {
      spinnerElement.remove();
    }, 20000);
  }

  private async removeSpinner(classname = 'my-loading-spinner'): Promise<void> {
    // Add the alert to the DOM
    this.node.querySelector(`.${classname}`)?.remove();
  }

  // private downloadButtonOrLink(): string {
  //   return '';
  // }
}
