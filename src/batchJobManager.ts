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
                <th class="text-center"><a id="add-job" role="button" class="btn btn-primary btn-xs">Add New</a></th>
            </tr>
        </thead>
        <tbody id="jobs-table-body">
        </tbody>
    </table>
</div>

<!-- Modal -->
<div class="modal fade" id="consoleLogModal" tabindex="-1" aria-labelledby="consoleLogModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-dialog-scrollable">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="consoleLogModalLabel">Console Log</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body" id="console-log-content">
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
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

  displayLog(logContent: string): void {
    const logModalContent = this.node.querySelector(
      '#logModalContent'
    ) as HTMLElement;
    logModalContent.textContent = logContent;
  }
}
