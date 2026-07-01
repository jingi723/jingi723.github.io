---
title: "개발 일지를 시작하며 — GitHub Pages + Chirpy로 블로그 세팅하기"
date: 2026-07-01 09:00:00 +0900
categories: [dev-log, blog]
tags: [swm, jekyll, chirpy, github-pages]     # 태그는 항상 소문자로
description: 개발하며 배우고 삽질한 것들을 기록으로 남기기로 했다. 첫 글은 이 블로그를 만든 과정.
pin: true
---

## 왜 개발 일지인가

머릿속으로만 정리한 지식은 생각보다 빨리 휘발된다.
같은 문제를 두 번, 세 번 다시 마주치면서 "분명 예전에 해결했는데..." 하는 경험을
반복하다 보니, 배운 것과 삽질한 과정을 **글로 남기기로** 했다.

이 블로그의 목표는 세 가지다.

- 문제 → 원인 → 해결까지의 **사고 흐름**을 남기기
- 나중의 내가 검색해서 바로 써먹을 수 있는 **레퍼런스** 만들기
- 설명하며 다시 이해하는 **학습 도구**로 쓰기

## 기술 스택

블로그 자체도 하나의 개발 프로젝트다. 선택한 스택은 다음과 같다.

| 항목 | 선택 | 이유 |
|------|------|------|
| 호스팅 | GitHub Pages | 무료, 리포 push 만으로 배포 |
| SSG | Jekyll | GitHub Pages 네이티브 지원 |
| 테마 | [Chirpy][chirpy] | 카테고리/태그/TOC/다크모드 기본 제공 |
| 빌드 | GitHub Actions | push 시 자동 빌드·배포 |

## 세팅 과정

### 1. 템플릿으로 리포 생성

Chirpy는 테마 저장소를 통째로 포크하는 대신,
`chirpy-starter` 템플릿으로 시작하는 것을 권장한다.
테마는 gem으로 받아 쓰기 때문에 업그레이드가 훨씬 깔끔하다.

```bash
gh repo create <username>.github.io \
  --template cotes2020/chirpy-starter \
  --public --clone
```

### 2. `_config.yml` 개인화

```yaml
lang: ko-KR
timezone: Asia/Seoul
title: jingi723
tagline: 개발하며 배운 것들을 기록합니다
url: "https://jingi723.github.io"
github:
  username: jingi723
```

### 3. 첫 글 작성

포스트는 `_posts/YYYY-MM-DD-title.md` 규칙으로 만든다.
프론트매터에서 특히 주의할 점 두 가지.

1. `date`에 타임존(`+0900`)을 꼭 넣기 — 안 넣으면 발행 시각이 어긋난다.
2. `tags`는 **항상 소문자** — 대소문자가 다르면 다른 태그로 취급된다.

> 카테고리는 최대 2단계(`[상위, 하위]`)까지 권장된다.
{: .prompt-tip }

### 4. 배포

`main` 브랜치에 push 하면 GitHub Actions가 빌드해서 배포한다.
저장소 **Settings → Pages → Source**를 `GitHub Actions`로 바꿔주면 끝.

## 앞으로

당장은 진행 중인 데이터 파이프라인 작업에서 마주친 문제들부터 정리해볼 생각이다.
꾸준함이 관건이니, 완벽한 글보다 **일단 기록하는 것**을 우선하려 한다.

[chirpy]: https://github.com/cotes2020/jekyll-theme-chirpy
