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

export function shortenId(job_id: string): string {
  const parts = job_id.split('-');
  return parts[0];
}

// This must agree with get_output_path() in utils.py
export function getOutputPath(meta: IBatchJobItem): string {
  const pathComponents = meta.file_path.split('/');
  const jobNameShort = getStem(meta.name);
  const filename = pathComponents.pop();
  const outputFilename = getOutputFilename(filename);
  const root = pathComponents.join('/');
  const jobIdShort = shortenId(meta.job_id);
  const parent_dir = `${jobNameShort}_${jobIdShort}`;
  const outputPath = `${root}/${parent_dir}`;
  return `${outputPath}/${outputFilename}`;
}

function getStem(filename: string): string {
  if (!filename.includes('.')) {
    return filename;
  }
  return filename.split('.').slice(0, -1).join('.');
}

function getOutputFilename(filename: string | undefined): string {
  if (!filename) {
    return 'undefined';
  }

  const xs = filename.split('.');
  if (xs[xs.length - 1] === 'ipynb') {
    return filename;
  } else {
    return getStem(filename) + '.out';
  }
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

function addOptionTag(instanceType: string): string {
  return `<option value="${instanceType}">${instanceType}</option>`;
}

export function toOptionTags(instanceTypes: string[]): string {
  const xs = instanceTypes.map(addOptionTag);
  return xs.join('\n      ');
}

// Convert '2023-04-17T22:35:35.299177+00:00' into '2023-04-17 22:35 (UTC)'
export function toDatetimeShort(timestamp: string): string {
  const date = timestamp.slice(0, 10);
  const hhmm = timestamp.slice(11, 16);
  return [date, hhmm, '(UTC)'].join(' ');
}
