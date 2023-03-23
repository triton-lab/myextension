import { Dialog, showDialog } from '@jupyterlab/apputils';
import { Widget } from '@lumino/widgets';

const SERVER_URL = '/myextension';

const TABLE = `
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
          <button type="button" class="btn btn-primary" data-bs-toggle="modal" data-bs-target="#batch-job-create-job">
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

export class BatchJobManager extends Widget {
  constructor() {
    console.log('BatchJobManager: constructor()!');
    super();
    this.node.innerHTML = TABLE;
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
            this.displayLog(logContent);
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
    const nameInput = document.createElement('input');
    const filePathInput = document.createElement('input');
    const instanceTypeSelect = document.createElement('select');

    nameInput.placeholder = 'Name';
    filePathInput.placeholder = 'File Path';

    const instanceTypes = [
      'c6a.large',
      'c6a.xlarge',
      'c6a.2xlarge',
      't3a.xlarge'
    ];

    instanceTypes.forEach(instanceType => {
      const option = document.createElement('option');
      option.value = instanceType;
      option.innerText = instanceType;
      instanceTypeSelect.appendChild(option);
    });

    body.node.appendChild(nameInput);
    body.node.appendChild(document.createElement('br'));
    body.node.appendChild(filePathInput);
    body.node.appendChild(document.createElement('br'));
    body.node.appendChild(instanceTypeSelect);

    const result = await showDialog({
      title: 'Create Job',
      body,
      buttons: [Dialog.cancelButton(), Dialog.okButton({ label: 'Create Job' })]
    });

    if (result.button.accept) {
      console.log('Name:', nameInput.value);
      console.log('File Path:', filePathInput.value);
      console.log('Instance Type:', instanceTypeSelect.value);
      // Perform your job creation logic here
    }
  }

  displayLog(logContent: string): void {
    const logModalContent = this.node.querySelector(
      '#logModalContent'
    ) as HTMLElement;
    logModalContent.textContent = logContent;
  }
}
