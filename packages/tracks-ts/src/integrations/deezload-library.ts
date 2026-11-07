import { access, constants, mkdir, readdir, rename, rm, rmdir, stat } from "node:fs/promises";
import { delimiter, extname, isAbsolute, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import type {
  BackendAvailability,
  IndividualDownload,
  TrackAcquirer,
} from "../acquire-manifest.js";
import { safeFilename } from "../acquisition.js";
import type {
  ScoredYoutubeCandidate,
  YoutubeSearchCandidate,
  YoutubeTarget,
} from "../acquisition.js";
import { JsonDiskCache, type CacheMode, type CacheStats } from "../cache.js";
import { TracksError } from "../errors.js";
import { runProcess, type RunProcessOptions, type ProcessResult } from "../process.js";

/** Deezer quality tiers understood by the deezload CLI, highest first. */
export const DEEZLOAD_QUALITIES = ["FLAC", "MP3_320", "MP3_128"] as const;
export type DeezloadQuality = (typeof DEEZLOAD_QUALITIES)[number];

export const DEEZLOAD_ARL_ENV = "DEEZLOAD_ARL";
export const DEEZLOAD_PATH_ENV = "DEEZLOAD_PATH";
export const DEEZLOAD_PYTHON_ENV = "DEEZLOAD_PYTHON";

const DEEZER_API_BASE_URL = "https://api.deezer.com/";
const DEEZER_TRACK_URL = "https://www.deezer.com/track/";
const DEFAULT_SEARCH_LIMIT = 10;
const DEFAULT_RETRIES = 3;
const DEFAULT_REQUEST_INTERVAL_MS = 150;
const AUDIO_EXTENSIONS = new Set([".flac", ".mp3"]);
const ANSI_ESCAPES = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "gu");

export type DeezloadCandidateMetadata = {
  trackId: string;
  matchedBy: "isrc" | "search";
  isrc?: string;
  album?: string;
  explicit?: boolean;
  requestedQuality: DeezloadQuality;
};

export type DeezloadSearchCandidate = YoutubeSearchCandidate & {
  /** Quality hint derived from the configured download tier and identity evidence. */
  sourceQualityScore: number;
  deezer: DeezloadCandidateMetadata;
};

export type DeezloadAcquirerOptions = {
  /** Deezer ARL cookie; defaults to the DEEZLOAD_ARL environment variable. */
  arl?: string;
  /** deezload executable or deezload.py path; defaults to DEEZLOAD_PATH, then `deezload` on PATH. */
  command?: string;
  /** Interpreter for a `.py` command; defaults to DEEZLOAD_PYTHON, then `python3`. */
  python?: string;
  environment?: Partial<
    Pick<
      NodeJS.ProcessEnv,
      typeof DEEZLOAD_ARL_ENV | typeof DEEZLOAD_PATH_ENV | typeof DEEZLOAD_PYTHON_ENV | "PATH"
    >
  >;
  /** Requested tier; deezload falls back to lower tiers unless `minQuality` forbids it. */
  quality?: DeezloadQuality;
  minQuality?: DeezloadQuality;
  retries?: number;
  searchLimit?: number;
  /** Kill a single track transfer after this long. */
  transferTimeoutMs?: number;
  apiBaseUrl?: string;
  requestIntervalMs?: number;
  cache?: JsonDiskCache | false;
  cacheMode?: CacheMode;
  searchTtlMs?: number;
  /** Cancels catalog searches and active deezload processes. */
  signal?: AbortSignal;
};

export type DeezloadAcquirerDependencies = {
  fetch: typeof globalThis.fetch;
  runProcess(
    command: string,
    args: readonly string[],
    options?: RunProcessOptions,
  ): Promise<ProcessResult>;
  /** Resolve a bare command name against PATH; returns undefined when it is not installed. */
  locateCommand(command: string, path: string | undefined): Promise<string | undefined>;
};

const defaultDependencies: DeezloadAcquirerDependencies = {
  fetch: (input, init) => globalThis.fetch(input, init),
  runProcess,
  locateCommand: locateExecutable,
};

/**
 * Deezer acquisition through the external `deezload` CLI (github.com/Lunatic16/deezload).
 *
 * Candidates come from the public Deezer catalog API, so searches never need the ARL. Downloads
 * spawn deezload per track with the ARL passed through its environment, stage into a per-track
 * scratch directory, then move the tagged FLAC or MP3 into the pipeline's staging basename.
 */
export class DeezloadAcquirer implements TrackAcquirer {
  readonly backend = "deezload" as const;
  readonly #arl: string | undefined;
  readonly #command: string | undefined;
  readonly #python: string;
  readonly #path: string | undefined;
  readonly #quality: DeezloadQuality;
  readonly #minQuality: DeezloadQuality | undefined;
  readonly #retries: number;
  readonly #searchLimit: number;
  readonly #transferTimeoutMs: number | undefined;
  readonly #apiBaseUrl: string;
  readonly #requestIntervalMs: number;
  readonly #cache: JsonDiskCache | undefined;
  readonly #cacheMode: CacheMode;
  readonly #searchTtlMs: number;
  readonly #signal: AbortSignal | undefined;
  readonly #dependencies: DeezloadAcquirerDependencies;
  readonly #searches = new Map<string, Promise<DeezloadSearchCandidate[]>>();
  #executable: Promise<readonly string[] | undefined> | undefined;
  #authenticationFailure: string | undefined;
  #nextRequestAt = 0;
  #requestQueue = Promise.resolve();

  constructor(
    options: DeezloadAcquirerOptions = {},
    dependencies: Partial<DeezloadAcquirerDependencies> = {},
  ) {
    const environment = options.environment ?? process.env;
    this.#arl = clean(options.arl ?? environment[DEEZLOAD_ARL_ENV]);
    this.#command = clean(options.command ?? environment[DEEZLOAD_PATH_ENV]);
    this.#python = clean(options.python ?? environment[DEEZLOAD_PYTHON_ENV]) ?? "python3";
    this.#path = environment.PATH ?? process.env.PATH;
    this.#quality = options.quality ?? "FLAC";
    this.#minQuality = options.minQuality;
    if (this.#minQuality && qualityRank(this.#minQuality) < qualityRank(this.#quality)) {
      throw new RangeError("minQuality must not exceed quality");
    }
    this.#retries = positiveInteger(options.retries, DEFAULT_RETRIES, "retries");
    this.#searchLimit = positiveInteger(options.searchLimit, DEFAULT_SEARCH_LIMIT, "searchLimit");
    this.#transferTimeoutMs = optionalPositiveInteger(
      options.transferTimeoutMs,
      "transferTimeoutMs",
    );
    this.#apiBaseUrl = new URL(options.apiBaseUrl ?? DEEZER_API_BASE_URL).toString();
    this.#requestIntervalMs = options.requestIntervalMs ?? DEFAULT_REQUEST_INTERVAL_MS;
    this.#cacheMode = options.cacheMode ?? "default";
    this.#cache =
      options.cache === false
        ? undefined
        : (options.cache ??
          new JsonDiskCache({
            namespace: "deezload-search-v1",
            ...(options.cacheMode ? { mode: options.cacheMode } : {}),
          }));
    this.#searchTtlMs = options.searchTtlMs ?? 6 * 60 * 60 * 1000;
    this.#signal = options.signal;
    this.#dependencies = { ...defaultDependencies, ...dependencies };
  }

  cacheStats(): CacheStats | undefined {
    return this.#cache?.snapshot();
  }

  async availability(): Promise<BackendAvailability> {
    if (!this.#arl) {
      return { available: false, reason: `${DEEZLOAD_ARL_ENV} is not configured` };
    }
    if (this.#authenticationFailure) {
      return { available: false, reason: this.#authenticationFailure };
    }
    const executable = await this.#resolveExecutable();
    if (!executable) {
      return {
        available: false,
        reason: this.#command
          ? `${DEEZLOAD_PATH_ENV} does not point at an existing deezload executable: ${this.#command}`
          : `deezload executable not found on PATH; install github.com/Lunatic16/deezload or set ${DEEZLOAD_PATH_ENV}`,
      };
    }
    return { available: true };
  }

  search(target: YoutubeTarget): Promise<DeezloadSearchCandidate[]> {
    const key = JSON.stringify({
      artists: target.artists,
      title: target.title,
      isrc: target.isrc,
      searchQuery: target.searchQuery,
    });
    const running = this.#searches.get(key);
    if (running) return running;
    const search = this.#search(target).finally(() => {
      if (this.#searches.get(key) === search) this.#searches.delete(key);
    });
    this.#searches.set(key, search);
    return search;
  }

  async #search(target: YoutubeTarget): Promise<DeezloadSearchCandidate[]> {
    this.#signal?.throwIfAborted();
    const candidates: DeezloadSearchCandidate[] = [];
    const isrc = normalizeIsrc(target.isrc);
    if (isrc) {
      const track = asRecord(await this.#cached(`track/isrc:${encodeURIComponent(isrc)}`));
      const exact = this.#candidateFromTrack(track, "isrc");
      if (exact && normalizeIsrc(exact.deezer.isrc) === isrc) candidates.push(exact);
    }
    const queries = target.searchQuery
      ? [target.searchQuery]
      : [
          `artist:"${target.artists.join(" ")}" track:"${target.title}"`,
          [...target.artists, target.title].join(" ").trim(),
        ];
    for (const query of queries) {
      const response = asRecord(
        await this.#cached("search", { q: query, limit: String(this.#searchLimit) }),
      );
      for (const item of array(response.data)) {
        const candidate = this.#candidateFromTrack(asRecord(item), "search");
        if (candidate) candidates.push(candidate);
      }
      // Field-qualified search is strict; only widen to a free-text query when it finds nothing.
      if (candidates.some((candidate) => candidate.deezer.matchedBy === "search")) break;
    }
    const seen = new Set<string>();
    return candidates
      .filter((candidate) => {
        if (seen.has(candidate.id)) return false;
        seen.add(candidate.id);
        return true;
      })
      .sort((left, right) => right.sourceQualityScore - left.sourceQualityScore);
  }

  #candidateFromTrack(
    track: Record<string, unknown>,
    matchedBy: DeezloadCandidateMetadata["matchedBy"],
  ): DeezloadSearchCandidate | undefined {
    const trackId = positiveId(track.id);
    const title = clean(track.title) ?? clean(track.title_short);
    if (!trackId || !title || track.readable === false) return undefined;
    const artist = clean(asRecord(track.artist).name);
    const album = clean(asRecord(track.album).title);
    const durationSeconds = positiveNumber(track.duration);
    const isrc = normalizeIsrc(clean(track.isrc));
    const explicit = typeof track.explicit_lyrics === "boolean" ? track.explicit_lyrics : undefined;
    const id = String(trackId);
    const sourceQualityScore = round(
      matchedBy === "isrc" ? 1 : qualityEvidence(this.#quality) * 0.9 + 0.05,
    );
    return {
      id,
      source: `${DEEZER_TRACK_URL}${id}`,
      title,
      ...(artist ? { artist } : {}),
      uploader: "Deezer",
      ...(durationSeconds === undefined ? {} : { durationSeconds }),
      sourceQualityScore,
      deezer: {
        trackId: id,
        matchedBy,
        ...(isrc ? { isrc } : {}),
        ...(album ? { album } : {}),
        ...(explicit === undefined ? {} : { explicit }),
        requestedQuality: this.#quality,
      },
    };
  }

  async download(
    candidate: ScoredYoutubeCandidate,
    destinationDirectory: string,
    basename: string,
  ): Promise<IndividualDownload> {
    this.#signal?.throwIfAborted();
    if (!this.#arl) throw new Error(`Deezer unavailable: ${DEEZLOAD_ARL_ENV} is not configured`);
    if (this.#authenticationFailure) {
      throw new Error(`Deezer unavailable: ${this.#authenticationFailure}`);
    }
    const trackId = deezerTrackId(candidate);
    await mkdir(destinationDirectory, { recursive: true });
    const safeBasename = safeFilename(basename);
    const existing = await completedOutput(destinationDirectory, safeBasename);
    if (existing) {
      return {
        path: existing,
        source: candidate.source,
        engine: "reference",
        temporary: true,
        transcoded: false,
        qualityNote: "Reused a completed deezload staging artifact.",
      };
    }

    const executable = await this.#resolveExecutable();
    if (!executable) throw new Error("Deezer unavailable: deezload executable not found");
    const scratch = join(destinationDirectory, ".deezload", trackId);
    await mkdir(scratch, { recursive: true });
    const [command, ...prefix] = executable;
    const args = [
      ...prefix,
      "--track-id",
      trackId,
      "--output",
      scratch,
      "--quality",
      this.#quality,
      ...(this.#minQuality ? ["--min-quality", this.#minQuality] : []),
      "--retries",
      String(this.#retries),
      "--quiet",
      "--no-color",
      "--no-lyrics",
    ];
    const signal = this.#downloadSignal();
    let result: ProcessResult;
    try {
      result = await this.#dependencies.runProcess(command!, args, {
        env: {
          [DEEZLOAD_ARL_ENV]: this.#arl,
          NO_COLOR: "1",
          PYTHONUNBUFFERED: "1",
          PYTHONIOENCODING: "utf-8",
        },
        ...(signal ? { signal } : {}),
      });
    } catch (error) {
      this.#signal?.throwIfAborted();
      if (error instanceof TracksError && error.code === "TOOL_MISSING") {
        this.#executable = Promise.resolve(undefined);
      }
      const message = redactArl(errorMessage(error), this.#arl);
      this.#noteAuthenticationFailure(message);
      throw new Error(`deezload failed for Deezer track ${trackId}: ${message}`, { cause: error });
    }
    this.#signal?.throwIfAborted();
    const transcript = `${result.stdout}\n${result.stderr}`;
    const produced = await producedAudioFile(scratch);
    if (!produced) {
      const detail = redactArl(failureDetail(transcript), this.#arl);
      this.#noteAuthenticationFailure(detail);
      throw new Error(
        `deezload did not produce audio for Deezer track ${trackId}${detail ? `: ${detail}` : ""}`,
      );
    }
    const extension = extname(produced).toLocaleLowerCase("en-US");
    const destination = resolve(join(destinationDirectory, `${safeBasename}${extension}`));
    await rename(produced, destination);
    await rm(scratch, { recursive: true, force: true });
    await removeEmptyDirectory(join(destinationDirectory, ".deezload"));
    return {
      path: destination,
      source: candidate.source,
      engine: "reference",
      temporary: true,
      transcoded: false,
      qualityNote: qualityNote(extension, this.#quality, transcript),
    };
  }

  #downloadSignal(): AbortSignal | undefined {
    const signals = [
      ...(this.#signal ? [this.#signal] : []),
      ...(this.#transferTimeoutMs ? [AbortSignal.timeout(this.#transferTimeoutMs)] : []),
    ];
    if (!signals.length) return undefined;
    return signals.length === 1 ? signals[0] : AbortSignal.any(signals);
  }

  #noteAuthenticationFailure(detail: string): void {
    if (/authentication (?:failed|error)|invalid arl|check your arl/iu.test(detail)) {
      this.#authenticationFailure = `Deezer rejected the configured ${DEEZLOAD_ARL_ENV}: ${detail}`;
    }
  }

  #resolveExecutable(): Promise<readonly string[] | undefined> {
    this.#executable ??= (async () => {
      if (this.#command) {
        if (!(await isFile(this.#command))) return undefined;
        return extname(this.#command).toLocaleLowerCase("en-US") === ".py"
          ? [this.#python, resolve(this.#command)]
          : [resolve(this.#command)];
      }
      const located = await this.#dependencies.locateCommand("deezload", this.#path);
      return located ? [located] : undefined;
    })();
    return this.#executable;
  }

  async #cached(path: string, query: Record<string, string> = {}): Promise<unknown> {
    const url = new URL(path, this.#apiBaseUrl);
    for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
    const load = () => this.#request(url);
    return this.#cache
      ? this.#cache.getOrLoad({ schemaVersion: 1, kind: "deezload-catalog", url: url.href }, load, {
          ttlMs: this.#searchTtlMs,
          mode: this.#cacheMode,
        })
      : load();
  }

  async #request(url: URL): Promise<unknown> {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await this.#pace();
      this.#signal?.throwIfAborted();
      const response = await this.#dependencies.fetch(url, {
        headers: { accept: "application/json" },
        ...(this.#signal ? { signal: this.#signal } : {}),
      });
      if ((response.status === 429 || response.status >= 500) && attempt < 2) {
        const retryAfter = Number(response.headers.get("retry-after"));
        await delay(
          Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 750 * (attempt + 1),
        );
        continue;
      }
      if (!response.ok) {
        throw new Error(
          `Deezer catalog request failed: HTTP ${String(response.status)} ${url.href}`,
        );
      }
      const value = (await response.json()) as unknown;
      const error = asRecord(asRecord(value).error);
      if (Object.keys(error).length) {
        // 800 is Deezer's "no data" code for unknown IDs and ISRCs.
        if (positiveNumber(error.code) === 800) return undefined;
        throw new Error(
          `Deezer catalog request failed: ${clean(error.message) ?? "unknown API error"}`,
        );
      }
      return value;
    }
    return undefined;
  }

  async #pace(): Promise<void> {
    let release!: () => void;
    const previous = this.#requestQueue;
    this.#requestQueue = new Promise<void>((resolveQueue) => {
      release = resolveQueue;
    });
    await previous;
    const waitMs = Math.max(0, this.#nextRequestAt - Date.now());
    if (waitMs > 0) await delay(waitMs);
    this.#nextRequestAt = Date.now() + this.#requestIntervalMs;
    release();
  }
}

/** Numeric Deezer track ID from a candidate's canonical track URL or bare numeric ID. */
export function deezerTrackId(candidate: Pick<YoutubeSearchCandidate, "id" | "source">): string {
  try {
    const url = new URL(candidate.source);
    const hostname = url.hostname.replace(/^www\./u, "").toLocaleLowerCase("en-US");
    const id = /^(?:\/[a-z]{2})?\/track\/(\d+)(?:\/|$)/u.exec(url.pathname)?.[1];
    if (hostname === "deezer.com" && id) return id;
  } catch {
    // Fall through to the bare ID check.
  }
  if (/^\d+$/u.test(candidate.id)) return candidate.id;
  throw new Error(`Invalid Deezer source: ${candidate.source}`);
}

export async function locateExecutable(
  command: string,
  path: string | undefined,
): Promise<string | undefined> {
  if (isAbsolute(command) || command.includes("/")) {
    return (await isExecutable(command)) ? command : undefined;
  }
  for (const directory of (path ?? "").split(delimiter).filter(Boolean)) {
    const candidate = join(directory, command);
    if (await isExecutable(candidate)) return candidate;
  }
  return undefined;
}

function qualityEvidence(quality: DeezloadQuality): number {
  switch (quality) {
    case "FLAC":
      return 1;
    case "MP3_320":
      return 0.72;
    case "MP3_128":
      return 0.38;
  }
}

function qualityRank(quality: DeezloadQuality): number {
  return DEEZLOAD_QUALITIES.indexOf(quality);
}

function qualityNote(extension: string, requested: DeezloadQuality, transcript: string): string {
  const fellBack = /falling back to/iu.test(transcript);
  const delivered = extension === ".flac" ? "lossless FLAC" : "MP3";
  return `Downloaded ${delivered} from Deezer with deezload${
    fellBack ? ` after ${requested} was unavailable for this track` : ""
  }.`;
}

function failureDetail(transcript: string): string {
  const lines = transcript
    .split(/\r?\n/u)
    .map((line) => line.replace(ANSI_ESCAPES, "").trim())
    .filter(Boolean);
  const errors = lines.filter((line) => /✗|error|failed|not available|unavailable/iu.test(line));
  return (errors.length ? errors : lines).slice(-3).join(" | ");
}

function redactArl(text: string, arl: string | undefined): string {
  return arl ? text.replaceAll(arl, "[REDACTED]") : text;
}

async function completedOutput(directory: string, basename: string): Promise<string | undefined> {
  for (const extension of AUDIO_EXTENSIONS) {
    const path = join(directory, `${basename}${extension}`);
    if (await nonemptyFile(path)) return resolve(path);
  }
  return undefined;
}

async function producedAudioFile(directory: string): Promise<string | undefined> {
  let entries: string[];
  try {
    entries = await readdir(directory);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  const audio = entries
    .filter((entry) => AUDIO_EXTENSIONS.has(extname(entry).toLocaleLowerCase("en-US")))
    .sort((left, right) => {
      const lossless = (name: string) =>
        extname(name).toLocaleLowerCase("en-US") === ".flac" ? 0 : 1;
      return lossless(left) - lossless(right) || left.localeCompare(right);
    });
  for (const entry of audio) {
    const path = join(directory, entry);
    if (await nonemptyFile(path)) return path;
  }
  return undefined;
}

async function removeEmptyDirectory(path: string): Promise<void> {
  try {
    await rmdir(path);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "ENOENT" && code !== "ENOTEMPTY") throw error;
  }
}

async function nonemptyFile(path: string): Promise<boolean> {
  try {
    const info = await stat(path);
    return info.isFile() && info.size > 0;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

async function isExecutable(path: string): Promise<boolean> {
  try {
    if (!(await stat(path)).isFile()) return false;
    await access(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeIsrc(value: string | undefined): string | undefined {
  const normalized = value?.replaceAll("-", "").trim().toLocaleUpperCase("en-US");
  return normalized && /^[A-Z]{2}[A-Z0-9]{3}\d{7}$/u.test(normalized) ? normalized : undefined;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function clean(value: unknown): string | undefined {
  const result = typeof value === "string" ? value.trim() : "";
  return result || undefined;
}

function positiveNumber(value: unknown): number | undefined {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function positiveId(value: unknown): number | undefined {
  const parsed = positiveNumber(value);
  return parsed && Number.isSafeInteger(parsed) ? parsed : undefined;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isInteger(result) || result <= 0) throw new RangeError(`${name} must be positive`);
  return result;
}

function optionalPositiveInteger(value: number | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  return positiveInteger(value, value, name);
}

function round(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}
