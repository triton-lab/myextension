import { Dialog, showDialog } from '@jupyterlab/apputils';
import { Widget } from '@lumino/widgets';

const SERVER_URL = '/myextension';

const JOB_TABLE = `
<div class="container mt-5">
  <table class="table table-striped">
    <thead>
      <tr>
        <th scope="col">Job ID</th>
        <th scope="col">Name</th>
        <th scope="col">Created At</th>
        <th scope="col">Status</th>
        <th scope="col">Actions</th>
        <th class="text-center">
          <button type="button" class="btn btn-primary">
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
    </select>
  </div>
</form>
`;

export class BatchJobManager extends Widget {
  constructor() {
    console.log('BatchJobManager: constructor()!');
    super();
    this.node.innerHTML = JOB_TABLE;
    this.title.label = 'Batch Jobs';
    this.title.closable = true;
    // https://jupyterlab.readthedocs.io/en/stable/developer/css.html
    this.addClass('jp-BatchJobManager');

    this.node
      .querySelector('.btn.btn-primary')
      ?.addEventListener('click', () => {
        this.showCreateJobDialog();
      });
  }

  onAfterAttach(): void {
    console.log('BatchJobManager: onAfterAttach()!');
    this.fetchJobs();
  }

  async fetchJobs(): Promise<void> {
    console.log('BatchJobManager: fetchJobs()!');

    try {
      const url = `${SERVER_URL}/jobs`;
      const response = await fetch(url);
      const jobs = await response.json();
      const tableBody = this.node.querySelector(
        '#jobs-table-body'
      ) as HTMLElement;
      tableBody.innerHTML = '';

      for (const job of jobs) {
        const row = document.createElement('tr');

        row.innerHTML = `
          <td>${job.job_id}</td>
          <td>${job.name}</td>
          <td>${job.created_at}</td>
          <td><a href="#" class="job-status">${job.status}</a></td>
          <td><button class="btn btn-danger btn-sm delete-job" data-job-id="${job.job_id}">Delete</button></td>
        `;

        tableBody.appendChild(row);
      }

      const deleteButtons = this.node.querySelectorAll('.delete-job');
      deleteButtons.forEach(button => {
        button.addEventListener('click', async event => {
          event.preventDefault();
          const jobId = (event.target as HTMLElement).dataset.jobId;
          if (jobId) {
            await this.deleteJob(jobId);
            this.fetchJobs();
          } else {
            console.warn('Job ID is not available');
          }
        });
      });

      const statusLinks = this.node.querySelectorAll('.job-status');
      statusLinks.forEach(link => {
        link.addEventListener('click', event => {
          event.preventDefault();
          const logContent = (event.target as HTMLElement).dataset.log;
          if (logContent) {
            console.log(`logContent: ${logContent}`);
          } else {
            console.warn('Log content is not available');
          }
        });
      });
    } catch (error) {
      console.error('Error fetching jobs:', error);
    }
  }

  async deleteJob(jobId: string): Promise<void> {
    try {
      const response = await fetch(`${SERVER_URL}/jobs/${jobId}`, {
        method: 'DELETE'
      });
      if (!response.ok) {
        throw new Error('Error deleting job');
      }
    } catch (error) {
      console.error('Error deleting job:', error);
    }
  }

  private async showCreateJobDialog(): Promise<void> {
    const body = new Widget();
    body.node.innerHTML = JOB_DEFINITION;
    const result = await showDialog({
      title: 'Create Job',
      body,
      buttons: [Dialog.cancelButton(), Dialog.okButton({ label: 'Create Job' })]
    });

    if (result.button.accept) {
      console.log(
        'Name:',
        (body.node.querySelector('#job-name') as HTMLSelectElement).value
      );
      console.log(
        'File Path:',
        (body.node.querySelector('#job-file-path') as HTMLSelectElement).value
      );
      console.log(
        'Instance Type:',
        (body.node.querySelector('#job-instance-type') as HTMLSelectElement)
          .value
      );
      // Perform your job creation logic here
    }
  }
}
