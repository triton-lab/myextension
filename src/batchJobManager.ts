import { Widget } from '@lumino/widgets';
import appHtml from '../style/app.html';

const SERVER_URL = '';

export class BatchJobManager extends Widget {
  constructor() {
    super();
    this.id = 'batch-job-manager';
    this.node.innerHTML = appHtml;
    this.addClass('batchJobManager');
    this.title.label = 'Batch Job Manager';
  }

  onAfterAttach(): void {
    this.fetchJobs();
  }

  async fetchJobs(): Promise<void> {
    try {
      const response = await fetch(`${SERVER_URL}/jobs`);
      const jobs = await response.json();
      const tableBody = this.node.querySelector(
        '#jobsTableBody'
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
