#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM25 + 倒排索引，用于表名/注释检索。纯标准库，无 numpy。
"""
import math
import re
from typing import Dict, List, Set, Tuple


_TOKEN_RE = re.compile(r"[a-z0-9_]+|[\u4e00-\u9fff]+", re.IGNORECASE)


def tokenize(text: str) -> List[str]:
    if not text:
        return []
    raw = [t.lower() for t in _TOKEN_RE.findall(text.lower())]
    out: List[str] = []

    def _push(tok: str):
        if tok and tok not in out:
            out.append(tok)

    for t in raw:
        # ASCII token: keep as-is
        if re.fullmatch(r"[a-z0-9_]+", t):
            _push(t)
            continue

        # Chinese sequence token: keep full phrase + char-level + 2/3-gram
        _push(t)
        chars = list(t)
        n = len(chars)
        if n == 1:
            _push(chars[0])
            continue

        # char-level fallback recall
        for ch in chars:
            _push(ch)

        # phrase-level precision (bi/tri-gram)
        for k in (2, 3):
            if n >= k:
                for i in range(0, n - k + 1):
                    _push("".join(chars[i:i + k]))

    return out


class BM25Index(object):
    """Okapi BM25 over pre-tokenized documents."""

    def __init__(self, doc_tokens: List[List[str]], k1: float = 1.2, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.doc_tokens = doc_tokens
        self.N = len(doc_tokens)
        self.doc_lens = [len(d) for d in doc_tokens]
        self.avgdl = (sum(self.doc_lens) / float(self.N)) if self.N else 0.0

        df: Dict[str, int] = {}
        inverted: Dict[str, Set[int]] = {}
        tfs: List[Dict[str, int]] = []

        for i, tokens in enumerate(doc_tokens):
            tf: Dict[str, int] = {}
            seen: Set[str] = set()
            for t in tokens:
                tf[t] = tf.get(t, 0) + 1
                if t not in seen:
                    seen.add(t)
                    df[t] = df.get(t, 0) + 1
                    inverted.setdefault(t, set()).add(i)
            tfs.append(tf)

        self.tfs = tfs
        self.df = df
        self.inverted = inverted
        self.idf: Dict[str, float] = {}
        for term, dfi in df.items():
            # BM25 idf variant
            self.idf[term] = math.log(1.0 + (self.N - dfi + 0.5) / (dfi + 0.5))

    def candidate_doc_ids(self, query_terms: List[str]) -> Set[int]:
        out: Set[int] = set()
        for t in query_terms:
            if t in self.inverted:
                out |= self.inverted[t]
        return out

    def score_doc(self, doc_id: int, query_terms: List[str]) -> float:
        if doc_id < 0 or doc_id >= self.N:
            return 0.0
        dl = self.doc_lens[doc_id]
        if dl == 0:
            return 0.0
        tf_d = self.tfs[doc_id]
        score = 0.0
        for q in query_terms:
            if q not in tf_d:
                continue
            idf = self.idf.get(q, 0.0)
            f = tf_d[q]
            denom = f + self.k1 * (1.0 - self.b + self.b * dl / self.avgdl) if self.avgdl > 0 else f + self.k1
            score += idf * (f * (self.k1 + 1.0)) / denom
        return score

    def search(self, query_terms: List[str], topn: int) -> List[Tuple[int, float]]:
        if not query_terms or self.N == 0:
            return []
        qterms = [t for t in query_terms if t]
        if not qterms:
            return []

        cands = self.candidate_doc_ids(qterms)
        if not cands:
            # 无交集时退化为扫全量（短查询偶发）
            cands = set(range(self.N))

        scored: List[Tuple[int, float]] = []
        for doc_id in cands:
            s = self.score_doc(doc_id, qterms)
            if s > 0:
                scored.append((doc_id, s))

        scored.sort(key=lambda x: (-x[1], x[0]))
        return scored[:topn]
