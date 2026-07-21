#!/usr/bin/env python3
"""Extrae publicaciones públicas de Instagram mediante Apify."""

from __future__ import annotations

import argparse
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
from queries_config import (
    INSTAGRAM_HASHTAGS,
    INSTAGRAM_PROFILE_URLS,
    INSTAGRAM_SEARCH_QUERIES,
)


ACTOR_ID = "apify/instagram-scraper"
REPO_ROOT = Path(__file__).resolve().parent.parent


def profile_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw.rstrip("/") + "/"
    return f"https://www.instagram.com/{raw.lstrip('@').strip('/')}/"


def hashtag_url(value: str) -> str:
    tag = value.strip().lstrip("#").replace(" ", "").lower()
    return f"https://www.instagram.com/explore/tags/{tag}/" if tag else ""


def build_plans(args: argparse.Namespace) -> list[ActorRunPlan]:
    profiles = [url for value in args.profiles if (url := profile_url(value))]
    hashtags = [url for value in args.hashtags if (url := hashtag_url(value))]
    plans: list[ActorRunPlan] = []
    base = {
        "resultsLimit": args.results_limit,
        "onlyPostsNewerThan": args.since,
        "addParentData": True,
    }
    if profiles:
        plans.extend([
            ActorRunPlan(
                "Perfiles oficiales · posts",
                {**base, "directUrls": profiles, "resultsType": "posts"},
                institutional=True,
                query="perfiles_oficiales",
            ),
            ActorRunPlan(
                "Perfiles oficiales · reels",
                {**base, "directUrls": profiles, "resultsType": "reels"},
                institutional=True,
                query="perfiles_oficiales",
            ),
            ActorRunPlan(
                "Etiquetas @ a perfiles oficiales",
                {**base, "directUrls": profiles, "resultsType": "mentions"},
                institutional=False,
                query="menciones_perfiles",
            ),
        ])
    if hashtags:
        plans.extend([
            ActorRunPlan(
                "Hashtags dirigidos · posts",
                {**base, "directUrls": hashtags, "resultsType": "posts"},
                query=",".join(args.hashtags),
            ),
            ActorRunPlan(
                "Hashtags dirigidos · reels",
                {**base, "directUrls": hashtags, "resultsType": "reels"},
                query=",".join(args.hashtags),
            ),
        ])
    if args.queries:
        plans.append(ActorRunPlan(
            "Descubrimiento por términos y usuarios",
            {
                "resultsType": "posts",
                "search": ", ".join(args.queries),
                "searchType": "user",
                "searchLimit": args.search_limit,
                "resultsLimit": args.results_limit,
                "onlyPostsNewerThan": args.since,
                "addParentData": True,
            },
            query=",".join(args.queries),
        ))
    return plans


def normalize_item(
    plan: ActorRunPlan,
    item: dict[str, Any],
    since: str,
    before: str,
) -> dict[str, Any] | None:
    raw_date = first(item, "timestamp", "takenAt", "createdAt", "date")
    if not in_window(raw_date, since, before):
        return None
    url = str(first(item, "url", "inputUrl"))
    return {
        "id": first(item, "id", "shortCode") or url,
        "tipo_registro": "publicacion_institucional" if plan.institutional else "mencion",
        "origen_busqueda": plan.name,
        "query_busqueda": first(item, "searchTerm") or plan.query,
        "input_url": first(item, "inputUrl"),
        "usuario": first(item, "ownerUsername", "owner.username", "username"),
        "fecha": normalize_datetime(raw_date),
        "texto": normalize_text(first(item, "caption", "text")),
        "url": url,
        "url_contexto": first(item, "inputUrl"),
        "likes": integer(first(item, "likesCount", "likes")),
        "comentarios": integer(first(item, "commentsCount", "comments")),
        "shares": integer(first(item, "sharesCount", "shares")),
        "vistas": integer(first(item, "videoViewCount", "videoPlayCount", "views")),
        "es_institucional": plan.institutional,
        "datos_originales_json": json_original(item),
    }


def normalize_items(
    plan: ActorRunPlan,
    item: dict[str, Any],
    since: str,
    before: str,
) -> list[dict[str, Any]]:
    parent = normalize_item(plan, item, since, before)
    if parent is None:
        return []
    rows = [parent]
    parent_url = parent["url"]
    latest_comments = first(item, "latestComments")
    if not isinstance(latest_comments, list):
        return rows
    for comment in latest_comments:
        if not isinstance(comment, dict):
            continue
        raw_date = first(comment, "timestamp", "createdAt")
        content = normalize_text(first(comment, "text"))
        if not content or not in_window(raw_date, since, before):
            continue
        comment_id = str(first(comment, "id"))
        rows.append({
            "id": comment_id or f"{parent['id']}:comentario:{len(rows)}",
            "tipo_registro": "comentario",
            "origen_busqueda": f"{plan.name} · comentario incluido",
            "query_busqueda": parent["query_busqueda"],
            "input_url": parent["input_url"],
            "usuario": first(comment, "ownerUsername", "owner.username"),
            "fecha": normalize_datetime(raw_date),
            "texto": content,
            "url": first(comment, "commentUrl") or parent_url,
            "url_contexto": parent_url,
            "likes": integer(first(comment, "likesCount", "likes")),
            "comentarios": integer(first(comment, "repliesCount")),
            "shares": 0,
            "vistas": 0,
            "es_institucional": False,
            "datos_originales_json": json_original(comment),
        })
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extractor Instagram vía Apify")
    parser.add_argument("--since", required=True, type=valid_date)
    parser.add_argument("--before", required=True, type=valid_date)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "Instagram"))
    parser.add_argument("--profile", dest="profiles", action="append", default=[])
    parser.add_argument("--hashtag", dest="hashtags", action="append", default=[])
    parser.add_argument("--query", dest="queries", action="append", default=[])
    parser.add_argument("--results-limit", type=int, default=50)
    parser.add_argument("--search-limit", type=int, default=3)
    parser.add_argument("--token", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-prompt", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.profiles:
        args.profiles = list(INSTAGRAM_PROFILE_URLS)
    if not args.hashtags:
        args.hashtags = list(INSTAGRAM_HASHTAGS)
    if not args.queries:
        args.queries = list(INSTAGRAM_SEARCH_QUERIES)
    return args


def main() -> None:
    load_repo_env()
    args = parse_args()
    validate_window(args.since, args.before)
    plans = build_plans(args)
    report_tag = build_report_tag(args.since, "Instagram")
    output_base = Path(args.output_dir)
    if args.dry_run:
        print_dry_run(ACTOR_ID, plans, output_base, report_tag)
        if not args.profiles:
            print("\n⚠️ Sin perfiles oficiales confirmados; el dry-run usa búsqueda y hashtags.")
        return
    token = require_token(args.token)
    raw_items = run_actor_plans(ACTOR_ID, plans, token)
    rows = [
        row
        for plan, item in raw_items
        for row in normalize_items(plan, item, args.since, args.before)
    ]
    outputs = write_social_outputs(rows, output_base, report_tag)
    print(f"✅ Instagram: {len(rows)} filas dentro de [{args.since}, {args.before})")
    for path in outputs.values():
        print(f"  - {path}")


if __name__ == "__main__":
    main()
