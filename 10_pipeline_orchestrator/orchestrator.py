"""
Project 10: Data Pipeline Orchestrator (CAPSTONE)
Author: mikey811
Description: A workflow orchestrator that chains all 9 projects into a unified
             data pipeline. Pulls raw data, cleans it, runs quality checks, loads
             to DB, generates reports, emails stakeholders — all in one command.
             Demonstrates production-grade error handling, dependency management,
             and retry logic.
"""

import sys
import os
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Callable, List, Dict, Any
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Import mock versions of previous projects (in production, these would be modules)
# For demonstration, we'll define lightweight task functions here

# ---------------------------------------------------------------------------
# TASK REGISTRY
# ---------------------------------------------------------------------------

class TaskResult:
    def __init__(self, success: bool, message: str, duration: float = 0.0, data: Any = None):
        self.success = success
        self.message = message
        self.duration = duration
        self.data = data


class Task:
    """Represents a single data pipeline stage."""
    def __init__(self, name: str, func: Callable, retry: int = 0, dependencies: List[str] = None):
        self.name = name
        self.func = func
        self.retry = retry
        self.dependencies = dependencies or []


# ---------------------------------------------------------------------------
# PIPELINE STAGES (Mock implementations)
# ---------------------------------------------------------------------------

def task_csv_intake(context: Dict[str, Any]) -> TaskResult:
    """Project 1: CSV Intake & Cleaner."""
    log.info("[TASK 1/9] CSV Intake & Cleaner")
    time.sleep(0.5)
    # Simulate reading and cleaning raw CSV
    context['cleaned_csv_path'] = 'data/cleaned_sales.csv'
    return TaskResult(success=True, message="CSV cleaned and validated", duration=0.5)


def task_quality_audit(context: Dict[str, Any]) -> TaskResult:
    """Project 2: Data Quality Audit."""
    log.info("[TASK 2/9] Data Quality Audit")
    time.sleep(0.3)
    # Simulate quality checks: null rate, schema, uniqueness
    context['quality_score'] = 97.3
    return TaskResult(success=True, message="Quality score: 97.3%", duration=0.3)


def task_api_to_database(context: Dict[str, Any]) -> TaskResult:
    """Project 3: API to Database."""
    log.info("[TASK 3/9] API to Database")
    time.sleep(0.6)
    # Simulate fetching from API and inserting to DB
    context['db_path'] = 'pipeline.db'
    return TaskResult(success=True, message="API data inserted: 1240 rows", duration=0.6)


def task_kpi_reporter(context: Dict[str, Any]) -> TaskResult:
    """Project 4: KPI Reporter."""
    log.info("[TASK 4/9] KPI Reporter")
    time.sleep(0.4)
    # Simulate generating KPI JSON
    context['kpi_report_path'] = 'reports/kpi_report.json'
    return TaskResult(success=True, message="KPI report generated", duration=0.4)


def task_folder_tracker(context: Dict[str, Any]) -> TaskResult:
    """Project 5: Folder Tracker."""
    log.info("[TASK 5/9] Folder Tracker")
    time.sleep(0.2)
    # Simulate logging file inventory
    return TaskResult(success=True, message="File inventory logged (42 files tracked)", duration=0.2)


def task_monitored_pipeline(context: Dict[str, Any]) -> TaskResult:
    """Project 6: Monitored Scheduled Pipeline."""
    log.info("[TASK 6/9] Monitored Scheduled Pipeline")
    time.sleep(0.5)
    # Simulate scheduled ETL run with monitoring
    context['run_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
    return TaskResult(success=True, message="Scheduled run logged with monitoring", duration=0.5)


def task_excel_dashboard(context: Dict[str, Any]) -> TaskResult:
    """Project 7: Excel/CSV Dashboard Generator."""
    log.info("[TASK 7/9] Excel Dashboard Generator")
    time.sleep(0.7)
    # Simulate building multi-tab Excel workbook
    context['dashboard_path'] = 'reports/dashboard.xlsx'
    return TaskResult(success=True, message="Dashboard created: 3 sheets, 1 chart", duration=0.7)


def task_db_migration(context: Dict[str, Any]) -> TaskResult:
    """Project 8: Database Migration Tool."""
    log.info("[TASK 8/9] Database Migration")
    time.sleep(0.5)
    # Simulate migrating staging -> production
    return TaskResult(success=True, message="Tables migrated (2 tables, 8452 rows)", duration=0.5)


def task_email_report(context: Dict[str, Any]) -> TaskResult:
    """Project 9: Automated Report Emailer."""
    log.info("[TASK 9/9] Automated Report Emailer")
    time.sleep(0.4)
    # Simulate sending email with attachments
    return TaskResult(success=True, message="Report emailed to 4 recipients", duration=0.4)


# ---------------------------------------------------------------------------
# ORCHESTRATOR
# ---------------------------------------------------------------------------

class PipelineOrchestrator:
    """Executes a DAG of tasks with dependency management and retry logic."""

    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.results: Dict[str, TaskResult] = {}
        self.context: Dict[str, Any] = {}

    def register_task(self, task: Task):
        self.tasks[task.name] = task

    def execute(self, dry_run: bool = False):
        """Execute all tasks in dependency order."""
        log.info("\n========== PIPELINE ORCHESTRATOR START ==========")
        log.info("Total tasks: %d", len(self.tasks))
        if dry_run:
            log.warning("[DRY RUN MODE] No tasks will be executed.")
            return

        start_time = time.time()
        executed = set()
        queue = list(self.tasks.keys())

        while queue:
            task_name = queue.pop(0)
            task = self.tasks[task_name]

            # Check dependencies
            if not all(dep in executed for dep in task.dependencies):
                queue.append(task_name)
                continue

            # Execute task with retry
            result = self._execute_with_retry(task)
            self.results[task_name] = result
            executed.add(task_name)

            if not result.success:
                log.error("[FAIL] %s: %s", task_name, result.message)
                log.error("Pipeline aborted due to task failure.")
                self._log_summary(time.time() - start_time)
                return False

            log.info("[OK] %s: %s (%.2fs)", task_name, result.message, result.duration)

        self._log_summary(time.time() - start_time)
        return True

    def _execute_with_retry(self, task: Task) -> TaskResult:
        for attempt in range(task.retry + 1):
            try:
                return task.func(self.context)
            except Exception as e:
                if attempt < task.retry:
                    log.warning("[RETRY %d/%d] %s failed: %s", attempt + 1, task.retry, task.name, e)
                    time.sleep(2 ** attempt)
                else:
                    return TaskResult(success=False, message=f"Error: {e}")

    def _log_summary(self, total_time: float):
        success = sum(1 for r in self.results.values() if r.success)
        failed  = len(self.results) - success

        log.info("\n========== PIPELINE SUMMARY ==========")
        log.info("Total time:    %.2f seconds", total_time)
        log.info("Tasks succeeded: %d", success)
        log.info("Tasks failed:    %d", failed)
        log.info("=======================================")


# ---------------------------------------------------------------------------
# PIPELINE DEFINITION
# ---------------------------------------------------------------------------

def build_pipeline() -> PipelineOrchestrator:
    orchestrator = PipelineOrchestrator()

    orchestrator.register_task(Task("csv_intake",       task_csv_intake,       retry=2))
    orchestrator.register_task(Task("quality_audit",    task_quality_audit,    dependencies=["csv_intake"]))
    orchestrator.register_task(Task("api_to_database",  task_api_to_database,  retry=3))
    orchestrator.register_task(Task("kpi_reporter",     task_kpi_reporter,     dependencies=["api_to_database"]))
    orchestrator.register_task(Task("folder_tracker",   task_folder_tracker))
    orchestrator.register_task(Task("monitored_pipeline", task_monitored_pipeline, dependencies=["kpi_reporter"]))
    orchestrator.register_task(Task("excel_dashboard",  task_excel_dashboard,  dependencies=["quality_audit"]))
    orchestrator.register_task(Task("db_migration",     task_db_migration,     dependencies=["api_to_database"]))
    orchestrator.register_task(Task("email_report",     task_email_report,     dependencies=["excel_dashboard", "kpi_reporter"]))

    return orchestrator


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Data Pipeline Orchestrator (Capstone)")
    parser.add_argument("--dry-run", action="store_true", help="Show execution plan without running tasks")
    args = parser.parse_args()

    orchestrator = build_pipeline()
    orchestrator.execute(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
