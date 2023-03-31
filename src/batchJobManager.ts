import { Dialog, showDialog, Notification } from '@jupyterlab/apputils';
import { FileDialog, IFileBrowserFactory } from '@jupyterlab/filebrowser';
import { Widget } from '@lumino/widgets';
import { ServerConnection } from '@jupyterlab/services';
import { URLExt } from '@jupyterlab/coreutils';

import { IBatchJobItem } from './types';
import { escapeHtmlAttribute } from './utils';

const SERVER_URL = '/myextension';

const JOB_TABLE = `
<div class="container mt-5">
  <div class="container ">
    <button type="button" class="btn btn-primary my-refresh">
    <i class="bi bi-arrow-clockwise"></i> Refresh
    </button>
  </div>
  <table class="table table-striped">
    <thead>
      <tr>
        <th scope="col">Job ID</th>
        <th scope="col">Name</th>
        <th scope="col">File At</th>
        <th scope="col">Created At</th>
        <th scope="col">Instance Type</th>
        <th scope="col">Status</th>
        <th scope="col">Actions</th>
        <th class="text-center">
          <button type="button" class="btn btn-primary my-create">
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
    <label for="job-instance-type" class="form-label">Instance Type</label>
    <select class="form-select" id="job-instance-type">
      <option value="c6a.large">c6a.large</option>
      <option value="c6a.xlarge">c6a.xlarge</option>
      <option value="c6a.2xlarge">c6a.2xlarge</option>
      <!-- Add more options as needed -->
      <option value="t3a.large">t3a.large</option>
      <option value="t3a.xlarge">t3a.xlarge</option>
      <option value="t3a.2xlarge">t3a.2xlarge</option>
      <option value="t3.medium">t3.medium</option>
    </select>
  </div>
</form>
`;

export class BatchJobManager extends Widget {
  constructor(private factory: IFileBrowserFactory) {
    super();
    console.log('BatchJobManager: constructor()!');
    this.node.innerHTML = JOB_TABLE;
    this.title.label = 'Batch Jobs';
    this.title.closable = true;
    // https://jupyterlab.readthedocs.io/en/stable/developer/css.html
    this.addClass('jp-BatchJobManager');

    this.node
      .querySelector('.btn.btn-primary.my-create')
      ?.addEventListener('click', () => {
        this.showCreateJobDialog();
      });

    this.node
      .querySelector('.btn.btn-primary.my-refresh')
      ?.addEventListener('click', () => {
        this.fetchJobs();
      });
  }

  onAfterAttach(): void {
    console.log('BatchJobManager: onAfterAttach()!');
    this.fetchJobs();
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
      throw new ServerConnection.ResponseError(response, response.statusText);
    }

    const jobs = (await response.json()) as IBatchJobItem[];
    const tableBody = this.node.querySelector(
      '#jobs-table-body'
    ) as HTMLElement;
    tableBody.innerHTML = '';

    for (const job of jobs) {
      const row = document.createElement('tr');
      const escaped_console = escapeHtmlAttribute(job.console_output);
      row.innerHTML = `
        <td>${job.job_id}</td>
        <td>${job.name}</td>
        <td>${job.file_path}</td>
        <td>${job.timestamp}</td>
        <td>${job.instance_type}</td>
        <td><a href="#" class="job-status" data-job-log="${escaped_console}">${job.status}</a></td>
        <td><button class="btn btn-danger btn-sm delete-job" data-job-id="${job.job_id}">Delete</button></td>
      `;

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
            await this.deleteJob(job_id);
            this.fetchJobs();
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
      throw new ServerConnection.ResponseError(response, response.statusText);
    }
  }

  async addJob(
    name: string,
    filePath: string,
    instanceType: string
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
          instance_type: instanceType
        })
      });
    } catch (error) {
      throw new ServerConnection.NetworkError(error as TypeError);
    }

    if (!response.ok) {
      console.error('Error adding a job.');
      throw new ServerConnection.ResponseError(response, response.statusText);
    }
  }

  private async showCreateJobDialog(): Promise<void> {
    const body = new Widget();
    body.node.innerHTML = JOB_DEFINITION;

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
      console.log('Name:', name);
      console.log('File Path:', filePath);
      console.log('Instance Type:', instanceType);

      try {
        await this.addJob(name, filePath, instanceType);
        this.fetchJobs();
      } catch (error) {
        let msg: string;
        if (error instanceof ServerConnection.ResponseError) {
          const detail = await error.response.json();
          msg = `Failed to Create Job: ${detail.data}`;
        } else if (error instanceof ServerConnection.NetworkError) {
          msg = `Failed to Create Job: ${error.message}`;
        } else {
          msg = `Failed to Create Job: ${error}`;
        }
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
}
