"""
HireLens – Data Pipeline
Orchestrates the full ETL flow:
  1. Scrape job postings from Indeed
  2. Deduplicate + validate
  3. Insert raw records into PostgreSQL
  4. Run NLP processing
  5. Upsert processed records + skill trend aggregates
  6. Log pipeline run metadata
"""

import os
from datetime import datetime
from typing import List, Optional

import pandas as pd
from loguru import logger
from tqdm import tqdm

from ..database import (
    JobPosting, ProcessedJob, SkillTrend, PipelineRun,
    get_db_session, init_db, get_engine,
)
from ..nlp import NLPProcessor
from ..scraper import IndeedScraper, RawJob


# ── Pipeline ───────────────────────────────────────────────────────────────────

class HireLensPipeline:
    """
    Full ETL pipeline: scrape → clean → store → NLP → aggregate.

    Args:
        db_url: PostgreSQL connection string. Falls back to DATABASE_URL env var.
        queries: List of search terms to scrape. Defaults to common data roles.
        locations: List of locations to search.
        max_per_query: Max jobs to scrape per (query, location) pair.
        use_spacy: Enable spaCy NLP (requires model installation).

    Example:
        pipeline = HireLensPipeline()
        pipeline.run()
    """

    def __init__(
        self,
        db_url: Optional[str] = None,
        queries: Optional[List[str]] = None,
        locations: Optional[List[str]] = None,
        max_per_query: int = int(os.getenv("MAX_JOBS_PER_RUN", 50)),
        use_spacy: bool = True,
    ):
        self.engine = get_engine(db_url)
        self.max_per_query = max_per_query
        self.queries = queries
        self.locations = locations
        self.nlp = NLPProcessor(use_spacy=use_spacy)
        init_db(self.engine)
        logger.info("HireLensPipeline initialised.")

    # ── Public ─────────────────────────────────────────────────────────────────

    def run(self) -> PipelineRun:
        """Execute the full pipeline and return the PipelineRun audit record."""
        run = self._start_run()
        try:
            # Step 1 – Scrape
            raw_jobs = self._scrape_jobs()
            run.jobs_scraped = len(raw_jobs)
            logger.info(f"Scraped {len(raw_jobs)} jobs.")

            # Step 2 – Ingest to DB
            new_ids = self._ingest_raw(raw_jobs)
            logger.info(f"Inserted {len(new_ids)} new job postings.")

            # Step 3 – NLP processing
            processed_count, error_count = self._process_jobs(new_ids)
            run.jobs_processed = processed_count
            run.errors = error_count

            # Step 4 – Aggregate skill trends
            self._update_skill_trends()

            run.status = "success"
            logger.info("Pipeline completed successfully.")
        except Exception as e:
            run.status = "failed"
            run.errors = (run.errors or 0) + 1
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            run.finished_at = datetime.utcnow()
            self._save_run(run)

        return run

    def run_nlp_only(self):
        """Re-process all unprocessed job postings (useful after schema/NLP changes)."""
        session = get_db_session(self.engine)
        try:
            unprocessed_ids = [
                r[0] for r in session.query(JobPosting.id)
                .filter(JobPosting.is_processed == False)
                .all()
            ]
            logger.info(f"Re-processing {len(unprocessed_ids)} unprocessed jobs.")
            processed, errors = self._process_jobs(unprocessed_ids)
            self._update_skill_trends()
            logger.info(f"NLP pass done: {processed} processed, {errors} errors.")
        finally:
            session.close()

    # ── Private ────────────────────────────────────────────────────────────────

    def _start_run(self) -> PipelineRun:
        run = PipelineRun(started_at=datetime.utcnow(), status="running")
        session = get_db_session(self.engine)
        try:
            session.add(run)
            session.commit()
            session.refresh(run)
            return run
        finally:
            session.close()

    def _save_run(self, run: PipelineRun):
        session = get_db_session(self.engine)
        try:
            db_run = session.get(PipelineRun, run.id)
            if db_run:
                db_run.finished_at = run.finished_at
                db_run.jobs_scraped = run.jobs_scraped
                db_run.jobs_processed = run.jobs_processed
                db_run.errors = run.errors
                db_run.status = run.status
                session.commit()
        finally:
            session.close()

    def _scrape_jobs(self) -> List[RawJob]:
        """Run Indeed scraper and collect all raw jobs."""
        raw_jobs = []
        with IndeedScraper() as scraper:
            for job in tqdm(
                scraper.scrape_all_roles(
                    queries=self.queries,
                    locations=self.locations,
                    max_per_query=self.max_per_query,
                ),
                desc="Scraping Indeed",
                unit="job",
            ):
                raw_jobs.append(job)
        return raw_jobs

    def _ingest_raw(self, raw_jobs: List[RawJob]) -> List[int]:
        """
        Insert new job postings into DB, skipping duplicates.
        Returns list of newly inserted posting IDs.
        """
        session = get_db_session(self.engine)
        new_ids = []
        try:
            # Fetch existing external IDs to skip
            existing_ids = {
                r[0] for r in session.query(JobPosting.external_id).all()
            }

            for job in raw_jobs:
                if job.external_id in existing_ids:
                    continue
                posting = JobPosting(
                    external_id=job.external_id,
                    source=job.source,
                    title=job.title,
                    company=job.company,
                    location=job.location,
                    is_remote=job.is_remote,
                    salary_min=job.salary_min,
                    salary_max=job.salary_max,
                    description_raw=job.description_raw,
                    url=job.url,
                    posted_date=job.posted_date,
                    scraped_at=job.scraped_at,
                )
                session.add(posting)
                existing_ids.add(job.external_id)

            session.commit()

            # Return IDs of unprocessed jobs
            new_ids = [
                r[0] for r in session.query(JobPosting.id)
                .filter(JobPosting.is_processed == False)
                .all()
            ]
        finally:
            session.close()
        return new_ids

    def _process_jobs(self, job_ids: List[int]) -> tuple:
        """Run NLP on unprocessed job postings. Returns (processed_count, error_count)."""
        if not job_ids:
            return 0, 0

        session = get_db_session(self.engine)
        processed = 0
        errors = 0

        try:
            for posting_id in tqdm(job_ids, desc="NLP Processing", unit="job"):
                try:
                    posting = session.get(JobPosting, posting_id)
                    if not posting:
                        continue

                    result = self.nlp.process(
                        title=posting.title,
                        description=posting.description_raw or "",
                    )

                    pjob = ProcessedJob(
                        posting_id=posting.id,
                        role_category=result["role_category"],
                        seniority=result["seniority"],
                        skills=result["skills"],
                        tools=result["tools"],
                        description_clean=result["description_clean"],
                    )
                    session.add(pjob)
                    posting.is_processed = True
                    processed += 1

                    # Commit in batches of 50
                    if processed % 50 == 0:
                        session.commit()
                        logger.debug(f"Committed batch ({processed} so far)")

                except Exception as e:
                    logger.warning(f"NLP error for posting {posting_id}: {e}")
                    errors += 1
                    continue

            session.commit()
        finally:
            session.close()

        return processed, errors

    def _update_skill_trends(self):
        """
        Recompute skill frequency counts and upsert into skill_trends table.
        Called after every pipeline run.
        """
        session = get_db_session(self.engine)
        try:
            # Pull all processed jobs with skills
            rows = session.query(ProcessedJob.skills, ProcessedJob.tools, ProcessedJob.role_category).all()
            if not rows:
                return

            # Build a flat records list
            records = []
            for skills, tools, category in rows:
                all_skills = list(skills or []) + list(tools or [])
                for skill in all_skills:
                    records.append({"skill": skill, "category": category})

            df = pd.DataFrame(records)
            if df.empty:
                return

            # Global counts
            global_counts = df.groupby("skill").size().reset_index(name="count")
            # Per-category counts
            category_counts = df.groupby(["skill", "category"]).size().reset_index(name="count")

            # Delete old trends
            session.query(SkillTrend).delete()

            # Insert global trends
            for _, row in global_counts.iterrows():
                session.add(SkillTrend(
                    skill=row["skill"], category="All", count=int(row["count"])
                ))

            # Insert per-category trends
            for _, row in category_counts.iterrows():
                session.add(SkillTrend(
                    skill=row["skill"], category=row["category"], count=int(row["count"])
                ))

            session.commit()
            logger.info(f"Skill trends updated: {len(global_counts)} unique skills.")
        finally:
            session.close()
