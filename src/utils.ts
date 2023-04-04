import { ContentsManager } from '@jupyterlab/services';
import { IBatchJobItem } from './types';

export function escapeHtmlAttribute(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function shortenId(job_id: string): string {
  const parts = job_id.split('-');
  return parts[0];
}

export function getOutputPath(meta: IBatchJobItem): string {
  const pathComponents = meta.file_path.split('/');
  const job_name = meta.name;
  const filename = pathComponents.pop();
  const root = pathComponents.join('/');
  const parent_dir = `${job_name}_${shortenId(meta.job_id)}`;
  const outputPath = `${root}/${parent_dir}`;
  return `${outputPath}/${filename}`;
}

export async function fileExists(relPath: string): Promise<boolean> {
  // Create a ContentsManager instance
  const contentsManager = new ContentsManager();

  try {
    // Check if the file exists by trying to get its metadata
    await contentsManager.get(relPath);
    return true;
  } catch (error) {
    const castError = error as any;

    if (castError.response && castError.response.status === 404) {
      // The file doesn't exist
      return false;
    }
    // Some other error occurred
    console.error(
      'An error occurred while checking if the file exists:',
      error
    );
    throw error;
  }
}
