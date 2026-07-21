#!/usr/bin/env python3
"""Extrae publicaciones públicas de TikTok mediante Apify."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from apify_social_common import (
    ActorRunPlan,
    first,
    in_window,
    integer,
    json_original,
    load_repo_env,
    normalize_datetime,
    normalize_text,
    print_dry_run,
    require_token,
    run_actor_plans,
    valid_date,
    validate_window,
    write_social_outputs,
)
from output_naming import build_report_tag
from queries_config import TIKTOK_HASHTAGS, TIKTOK_PROFILES, TIKTOK_SEARCH_QUERIES


ACTOR_ID = "clockworks/tiktok-scraper"
REPO_ROOT = Path(__file__).resolve().parent.parent


def search_date_filter(since: str, before: str) -> str:
    today = date.today()
    start = date.fromisoformat(since)
    end = date.fromisoformat(before)
    if start <= today <= end + timedelta(days=1) and (today - start).days <= 7:
        return "PAST_WEEK"
    return "ALL_TIME"


def build_plans(args: argparse.Namespace) -> list[ActorRunPlan]:
    common = {
        "resultsPerPage": args.results_limit,
        "commentsPerPost": 0,
        "proxyCountryCode": "MX",
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSlideshowImages": False,
        "downloadSubtitlesOptions": "NEVER_DOWNLOAD_SUBTITLES",
    }
    plans: list[ActorRunPlan] = []
    if args.profiles:
        plans.append(ActorRunPlan(
            "Perfiles oficiales · videos",
            {
                **common,
                "profiles": [value.lstrip("@").strip() for value in args.profiles],
                "profileScrapeSections": ["videos"],
                "profileSorting": "latest",
                "excludePinnedPosts": True,
                "oldestPostDateUnified": args.since,
                "newestPostDate": (date.fromisoformat(args.before) - timedelta(days=1)).isoformat(),
            },
            institutional=True,
            query="perfiles_oficiales",
        ))
    if args.queries:
        plans.append(ActorRunPlan(
            "Búsquedas temáticas · videos recientes",
            {
                **common,
                "searchQueries": args.queries,
                "searchSection": "/video",
                "videoSearchSorting": "LATEST",
                "videoSearchDateFilter": search_date_filter(args.since, args.before),
                "scrapeRelatedSearchWords": False,
            },
            query=",".join(args.queries),
        ))
    if args.hashtags:
        plans.append(ActorRunPlan(
            "Hashtags dirigidos",
            {**common, "hashtags": [value.lstrip("#") for value in args.hashtags]},
            query=",".join(args.hashtags),
        ))
    return plans


def normalize_item(
    plan: ActorRunPlan,
    item: dict[str, Any],
    since: str,
    before: str,
) -> dict[str, Any] | None:
    raw_date = first(item, "createTimeISO", "createTime", "timestamp")
    if not in_window(raw_date, since, before):
        return None
    url = str(first(item, "webVideoUrl", "url"))
    return {
        "id": first(item, "id") or url,
        "tipo_registro": "publicacion_institucional" if plan.institutional else "mencion",
        "origen_busqueda": plan.name,
        "query_busqueda": first(item, "searchQuery", "input") or plan.query,
        "input_url": first(item, "input"),
        "usuario": first(item, "authorMeta.name", "authorMeta.nickName", "author"),
        "fecha": normalize_datetime(raw_date),
        "texto": normalize_text(first(item, "text", "desc")),
        "url": url,
        "url_contexto": first(item, "authorMeta.profileUrl"),
        "likes": integer(first(item, "diggCount", "likes")),
        "comentarios": integer(first(item, "commentCount", "comments")),
        "shares": integer(first(item, "shareCount", "shares")),
        "vistas": integer(first(item, "playCount", "views")),
        "es_institucional": plan.institutional,
        "datos_originales_json": json_original(item),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extractor TikTok vía Apify")
    parser.add_argument("--since", required=True, type=valid_date)
    parser.add_argument("--before", required=True, type=valid_date)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "TikTok"))
    parser.add_argument("--profile", dest="profiles", action="append", default=[])
    parser.add_argument("--hashtag", dest="hashtags", action="append", default=[])
    parser.add_argument("--query", dest="queries", action="append", default=[])
    parser.add_argument("--results-limit", type=int, default=50)
    parser.add_argument("--token", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-prompt", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.profiles:
        args.profiles = list(TIKTOK_PROFILES)
    if not args.hashtags:
        args.hashtags = list(TIKTOK_HASHTAGS)
    if not args.queries:
        args.queries = list(TIKTOK_SEARCH_QUERIES)
    return args


def main() -> None:
    load_repo_env()
    args = parse_args()
    validate_window(args.since, args.before)
    plans = build_plans(args)
    report_tag = build_report_tag(args.since, "TikTok")
    output_base = Path(args.output_dir)
    if args.dry_run:
        print_dry_run(ACTOR_ID, plans, output_base, report_tag)
        if not args.profiles:
            print("\n⚠️ Sin perfiles oficiales confirmados; el dry-run usa búsquedas y hashtags.")
        return
    token = require_token(args.token)
    raw_items = run_actor_plans(ACTOR_ID, plans, token)
    rows = [row for plan, item in raw_items if (row := normalize_item(plan, item, args.since, args.before))]
    outputs = write_social_outputs(rows, output_base, report_tag)
    print(f"✅ TikTok: {len(rows)} filas dentro de [{args.since}, {args.before})")
    for path in outputs.values():
        print(f"  - {path}")


if __name__ == "__main__":
    main()
