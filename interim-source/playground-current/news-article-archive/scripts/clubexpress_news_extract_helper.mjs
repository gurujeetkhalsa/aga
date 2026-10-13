import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, writeFile, copyFile, stat } from "node:fs/promises";
import path from "node:path";

const CLUB_ID = "454497";
const MANAGER_URL = `https://usgo.org/content.aspx?page_id=27&club_id=${CLUB_ID}&actr=2`;

export async function collectRowsForNextSample(options) {
  const {
    tab,
    outputDir,
    previousSampleDir,
    previousSampleDirs = [],
    sampleSize = 100,
    maxPages = 12,
    extraSkipIds = [],
    minActiveDate = null,
    maxActiveDate = null,
    stopAtFirstNotVisible = false,
  } = options;
  const priorSampleDirs = previousSampleDirs.length ? previousSampleDirs : (previousSampleDir ? [previousSampleDir] : []);
  const skipIds = await buildSkipIds(priorSampleDirs, extraSkipIds);
  const minActiveDateKey = dateKeyFromIsoDate(minActiveDate);
  const maxActiveDateKey = dateKeyFromIsoDate(maxActiveDate);
  const skippedRows = [];
  const selectedRows = [];
  const sourcePages = [];
  const seenSelected = new Set();
  let reachedBeforeMinActiveDate = false;
  let stoppedAtNotVisible = false;
  let firstNotVisibleRow = null;

  await ensureManagerSorted(tab);

  for (let pageNumber = 1; pageNumber <= maxPages && selectedRows.length < sampleSize; pageNumber += 1) {
    const pageRows = await collectCurrentManagerRows(tab, pageNumber);
    sourcePages.push(`Page ${pageRows.currentPage || pageNumber} of ${pageRows.totalPages || "?"}`);

    for (const row of pageRows.rows) {
      if (!row.itemId) {
        continue;
      }
      const activeDateKey = dateKeyFromClubExpressDate(row.activeDate);
      if ((minActiveDateKey || maxActiveDateKey) && !activeDateKey) {
        skippedRows.push({
          itemId: row.itemId,
          headline: row.headline,
          author: row.author,
          activeDate: row.activeDate,
          reason: "active_date_unparsed",
        });
        continue;
      }
      if (maxActiveDateKey && activeDateKey > maxActiveDateKey) {
        if (!skippedRows.some((item) => item.itemId === row.itemId)) {
          skippedRows.push({
            itemId: row.itemId,
            headline: row.headline,
            author: row.author,
            activeDate: row.activeDate,
            reason: "after_max_active_date",
          });
        }
        continue;
      }
      if (minActiveDateKey && activeDateKey < minActiveDateKey) {
        reachedBeforeMinActiveDate = true;
        break;
      }
      if (stopAtFirstNotVisible && !isManagerRowVisible(row)) {
        stoppedAtNotVisible = true;
        firstNotVisibleRow = row;
        break;
      }
      if (skipIds.has(row.itemId)) {
        if (!skippedRows.some((item) => item.itemId === row.itemId)) {
          skippedRows.push({
            itemId: row.itemId,
            headline: row.headline,
            author: row.author,
            activeDate: row.activeDate,
            reason: "already_archived_or_explicitly_skipped",
          });
        }
        continue;
      }
      if (seenSelected.has(row.itemId)) {
        continue;
      }
      selectedRows.push(row);
      seenSelected.add(row.itemId);
      if (selectedRows.length >= sampleSize) {
        break;
      }
    }

    if (selectedRows.length >= sampleSize || reachedBeforeMinActiveDate || stoppedAtNotVisible) {
      break;
    }
    const moved = await gotoNextManagerPage(tab);
    if (!moved) {
      break;
    }
  }

  await mkdir(outputDir, { recursive: true });
  await writeJson(path.join(outputDir, "manager-index.json"), {
    extractedAt: new Date().toISOString(),
    source: {
      managerUrl: MANAGER_URL,
      searchResultsCount: await readManagerResultCount(tab),
      sort: "Active Date descending",
      status: "All",
      minActiveDate,
      maxActiveDate,
      reachedBeforeMinActiveDate,
      stopAtFirstNotVisible,
      stoppedAtNotVisible,
      firstNotVisibleRow,
    },
    skippedRows,
    rows: selectedRows,
  });

  return {
    skipIds: Array.from(skipIds).sort(),
    skippedRows,
    rows: selectedRows,
    sourcePages,
    reachedBeforeMinActiveDate,
    stoppedAtNotVisible,
    firstNotVisibleRow,
  };
}

export async function extractArticleBatch(options) {
  const {
    tab,
    rows,
    outputDir,
    sampleType = "recent-native-clubexpress-era-next-100",
    requestedSampleSize = rows.length,
    managerSearchResultsCount = null,
    sourcePages = [],
    skippedRows = [],
    startOrdinal = 1,
    progress = () => {},
  } = options;

  await mkdir(outputDir, { recursive: true });

  const existingManifest = await readJsonIfExists(path.join(outputDir, "sample-manifest.json"));
  const articles = Array.isArray(existingManifest?.articles) ? existingManifest.articles : [];
  const completedIds = new Set(articles.map((article) => String(article.itemId || article.folder || "")));
  const extractedAt = existingManifest?.extractedAt || new Date().toISOString();

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (completedIds.has(row.itemId)) {
      progress({ kind: "skip-existing", itemId: row.itemId, index: index + 1, total: rows.length });
      continue;
    }

    progress({ kind: "start", itemId: row.itemId, headline: row.headline, index: index + 1, total: rows.length });
    const articleDir = path.join(outputDir, row.folder || row.itemId);
    await mkdir(articleDir, { recursive: true });
    await writeJson(path.join(articleDir, "manager-row.json"), row);

    const configure = await extractConfigureForm(tab, row.configureUrl);
    await writeJson(path.join(articleDir, "configure-form.json"), configure);

    const editor = await extractEditorSource(tab, row.editTextUrl);
    await writeJson(path.join(articleDir, "editor-source.json"), editor.metadata);
    await writeFile(path.join(articleDir, "editor-source.raw-urlencoded.txt"), editor.raw, "utf8");
    await writeFile(path.join(articleDir, "editor-source.html"), editor.html, "utf8");

    const rendered = await extractRenderedArticle(tab, row.viewUrl, row.itemId);
    await writeJson(path.join(articleDir, "rendered-article.json"), rendered.metadata);
    await writeFile(path.join(articleDir, "rendered-container.html"), rendered.containerHtml, "utf8");
    await writeFile(path.join(articleDir, "rendered-article.html"), rendered.articleHtml, "utf8");

    const assetManifest = await captureArticleAssets(tab, articleDir, rendered.metadata.articleImages);
    await writeJson(path.join(articleDir, "asset-manifest.json"), assetManifest);

    const summary = buildArticleSummary({
      row,
      configure,
      editor: editor.metadata,
      rendered: rendered.metadata,
      assetManifest,
      ordinal: startOrdinal + articles.length,
    });
    articles.push(summary);
    completedIds.add(row.itemId);

    await writeManifest(outputDir, {
      extractedAt,
      outputDirectory: outputDir,
      managerSearchResultsCount: managerSearchResultsCount ?? existingManifest?.managerSearchResultsCount ?? null,
      sampleType,
      requestedSampleSize,
      sampleSize: articles.length,
      rowSelection: {
        sort: "Active Date descending",
        status: "All",
        sourcePages,
        skippedRows,
      },
      articles,
      completed: articles.length >= requestedSampleSize,
      completedCount: articles.length,
      lastUpdatedAt: new Date().toISOString(),
    });

    progress({ kind: "done", itemId: row.itemId, completed: articles.length, total: rows.length });
  }

  await writeManifest(outputDir, {
    extractedAt,
    outputDirectory: outputDir,
    managerSearchResultsCount: managerSearchResultsCount ?? existingManifest?.managerSearchResultsCount ?? null,
    sampleType,
    requestedSampleSize,
    sampleSize: articles.length,
    rowSelection: {
      sort: "Active Date descending",
      status: "All",
      sourcePages,
      skippedRows,
    },
    articles,
    notes: [
      "Read-only sample captured through authorized Chrome admin session.",
      "No ClubExpress Save/Delete/Share/Publish actions were used.",
      "Configure metadata, RadEditor source HTML, and rendered article HTML are captured per article.",
      "ClubExpress-managed article images are captured through pageAssets when available.",
      "External image URLs are recorded but not fetched in this sample.",
    ],
    completed: articles.length >= requestedSampleSize,
    lastUpdatedAt: new Date().toISOString(),
    completedCount: articles.length,
  });

  return { completedCount: articles.length, outputDir };
}

async function ensureManagerSorted(tab) {
  const url = await tab.url();
  if (url !== MANAGER_URL) {
    await tab.goto(MANAGER_URL);
    await tab.playwright.waitForLoadState({ state: "domcontentloaded", timeoutMs: 30000 });
  }

  const allLabel = tab.playwright.locator('label[for="ctl00_ctl00_status_all_search"]');
  const activeLabel = tab.playwright.locator('label[for="ctl00_ctl00_sort_active"]');
  const descLabel = tab.playwright.locator('label[for="ctl00_ctl00_sort_desc"]');
  const searchButton = tab.playwright.locator("#ctl00_ctl00_search_button");
  const counts = {
    allLabel: await allLabel.count(),
    activeLabel: await activeLabel.count(),
    descLabel: await descLabel.count(),
    searchButton: await searchButton.count(),
  };
  if (counts.allLabel !== 1 || counts.activeLabel !== 1 || counts.descLabel !== 1 || counts.searchButton !== 1) {
    throw new Error(`Could not find manager search controls: ${JSON.stringify(counts)}`);
  }
  await allLabel.click({});
  await activeLabel.click({});
  await descLabel.click({});
  await tab.playwright.expectNavigation(() => searchButton.click({}), {
    timeoutMs: 30000,
    waitUntil: "domcontentloaded",
  });
  await tab.playwright.locator("table.manager-table").waitFor({ state: "attached", timeoutMs: 30000 }).catch(() => {});

  const after = await readManagerSearchState(tab);
  if (!after.statusAll || !after.sortActive || !after.sortDesc || !after.hasResults) {
    throw new Error(`Manager search did not reach expected state: ${JSON.stringify(after)}`);
  }
  await resetToFirstManagerPage(tab);
}

async function readManagerSearchState(tab) {
  return await tab.playwright.evaluate(() => ({
    title: document.title,
    url: location.href,
    statusAll: Boolean(document.querySelector("#ctl00_ctl00_status_all_search")?.checked),
    sortActive: Boolean(document.querySelector("#ctl00_ctl00_sort_active")?.checked),
    sortDesc: Boolean(document.querySelector("#ctl00_ctl00_sort_desc")?.checked),
    hasResults: Boolean(document.querySelector("table.manager-table")),
  }), undefined, { timeoutMs: 10000 });
}

async function collectCurrentManagerRows(tab, pageNumber) {
  return await tab.playwright.evaluate((arg) => {
    function clean(value) {
      return (value || "").replace(/\s+/g, " ").trim();
    }
    function popupUrlFromOnclick(onclick) {
      const match = String(onclick || "").match(/openModalPopup\('([^']+)'/);
      if (!match) {
        return "";
      }
      return new URL(match[1].replace(/&amp;/g, "&"), location.origin).href;
    }
    function itemIdFromText(text) {
      const match = String(text || "").match(/[?&]item_id=(\d+)/);
      return match ? match[1] : "";
    }
    function summarizeVisibilityCell(cell) {
      const links = Array.from(cell?.querySelectorAll("a,img,span,i") || []);
      const attributes = links.map((el) => ({
        ariaLabel: el.getAttribute("aria-label") || "",
        className: el.getAttribute("class") || "",
        role: el.getAttribute("role") || "",
        title: el.getAttribute("title") || "",
        text: clean(el.innerText || el.textContent || ""),
      }));
      const raw = [
        clean(cell?.innerText || ""),
        clean(cell?.getAttribute("aria-label") || ""),
        clean(cell?.getAttribute("title") || ""),
        clean(cell?.className || ""),
        clean(attributes.map((item) => Object.values(item).join(" ")).join(" ")),
        clean(cell?.innerHTML || ""),
      ].filter(Boolean).join(" ");
      return {
        html: cell?.innerHTML || "",
        raw,
        text: clean(cell?.innerText || ""),
      };
    }

    const pageInfo = inferCurrentPage();
    const table = document.querySelector("table.manager-table");
    const rows = [];
    if (table) {
      Array.from(table.querySelectorAll("tr")).slice(1).forEach((tr, index) => {
        const cells = Array.from(tr.cells || []);
        if (cells.length < 9) {
          return;
        }
        const edit = tr.querySelector("a.edit-link");
        const config = tr.querySelector("a.config-link");
        const view = tr.querySelector("a.view-link");
        const editTextUrl = popupUrlFromOnclick(edit?.getAttribute("onclick"));
        const configureUrl = popupUrlFromOnclick(config?.getAttribute("onclick"));
        const viewUrl = view?.href || "";
        const itemId = itemIdFromText(editTextUrl) || itemIdFromText(configureUrl) || itemIdFromText(viewUrl);
        const visibilityCell = summarizeVisibilityCell(cells[5]);
        rows.push({
          activeDate: clean(cells[6]?.innerText),
          author: clean(cells[1]?.innerText),
          category: clean(cells[3]?.innerText),
          configureUrl,
          editTextUrl,
          expires: clean(cells[7]?.innerText),
          headline: clean(cells[0]?.innerText),
          itemId,
          rowIndex: String(index),
          sourcePage: `page-${pageInfo.currentPage || arg.pageNumber}`,
          status: clean(cells[4]?.innerText),
          subgroup: clean(cells[2]?.innerText),
          viewUrl,
          visible: visibilityCell.text,
          visibleCellHtml: visibilityCell.html,
          visibleCellRaw: visibilityCell.raw,
        });
      });
    }
    return {
      currentPage: pageInfo.currentPage || arg.pageNumber,
      totalPages: pageInfo.totalPages,
      rows,
    };

    function inferCurrentPage() {
      const previous = Array.from(document.querySelectorAll("a.previous"))
        .find((link) => link.getAttribute("onclick"));
      const next = Array.from(document.querySelectorAll("a.next"))
        .find((link) => link.getAttribute("onclick"));
      const prevMatch = String(previous?.getAttribute("onclick") || "").match(/goToPage\('(\d+)'\)/);
      const nextMatch = String(next?.getAttribute("onclick") || "").match(/goToPage\('(\d+)'\)/);
      let currentPage = null;
      if (prevMatch) {
        currentPage = Number(prevMatch[1]) + 1;
      } else if (nextMatch) {
        currentPage = Math.max(1, Number(nextMatch[1]) - 1);
      } else {
        currentPage = 1;
      }
      const bodyText = clean(document.body?.innerText || "");
      const pageMatches = Array.from(bodyText.matchAll(/Page\s+\d+\s+of\s+(\d+)/gi));
      const totalPages = pageMatches.length ? Number(pageMatches[0][1]) : null;
      return { currentPage, totalPages };
    }
  }, { pageNumber }, { timeoutMs: 10000 });
}

async function resetToFirstManagerPage(tab) {
  for (let attempt = 0; attempt < 350; attempt += 1) {
    const pageInfo = await tab.playwright.evaluate(() => {
      const previous = Array.from(document.querySelectorAll("a.previous"))
        .find((link) => link.getAttribute("onclick"));
      const next = Array.from(document.querySelectorAll("a.next"))
        .find((link) => link.getAttribute("onclick"));
      const prevMatch = String(previous?.getAttribute("onclick") || "").match(/goToPage\('(\d+)'\)/);
      const nextMatch = String(next?.getAttribute("onclick") || "").match(/goToPage\('(\d+)'\)/);
      let currentPage = 1;
      if (prevMatch) {
        currentPage = Number(prevMatch[1]) + 1;
      } else if (nextMatch) {
        currentPage = Math.max(1, Number(nextMatch[1]) - 1);
      }
      return {
        currentPage,
        hasPrevious: Boolean(prevMatch),
      };
    }, undefined, { timeoutMs: 10000 });
    if (!pageInfo.hasPrevious || pageInfo.currentPage <= 1) {
      return;
    }
    const previousLink = tab.playwright.locator("a.previous");
    const count = await previousLink.count();
    if (count < 1) {
      return;
    }
    await tab.playwright.expectNavigation(() => previousLink.nth(0).click({}), {
      timeoutMs: 30000,
      waitUntil: "domcontentloaded",
    });
  }
  throw new Error("Unable to reset News Manager pagination to page 1.");
}

async function gotoNextManagerPage(tab) {
  const state = await tab.playwright.evaluate(() => {
    const next = document.querySelector("a.next");
    return {
      exists: Boolean(next),
      href: next?.getAttribute("href") || "",
      text: next?.textContent || "",
      className: next?.className || "",
      onclick: next?.getAttribute("onclick") || "",
    };
  }, undefined, { timeoutMs: 10000 });
  if (!state.exists || !state.onclick || /\bdisabled\b/i.test(state.className)) {
    return false;
  }
  const nextLink = tab.playwright.locator("a.next");
  const count = await nextLink.count();
  if (count < 1) {
    throw new Error("Expected at least one next link, found none");
  }
  await tab.playwright.expectNavigation(() => nextLink.nth(0).click({}), {
    timeoutMs: 30000,
    waitUntil: "domcontentloaded",
  });
  return true;
}

async function readManagerResultCount(tab) {
  return await tab.playwright.evaluate(() => {
    const text = (document.body?.innerText || "").replace(/\s+/g, " ");
    const match = text.match(/Search Results\s*\((\d+)\s+news article/i)
      || text.match(/(\d+)\s+Search Results/i)
      || text.match(/Search Results\s+(\d+)/i);
    return match ? match[1] : null;
  }, undefined, { timeoutMs: 10000 });
}

async function extractConfigureForm(tab, url) {
  await tab.goto(url);
  await tab.playwright.waitForLoadState({ state: "domcontentloaded", timeoutMs: 30000 });
  return await tab.playwright.evaluate(() => {
    function clean(value) {
      return (value || "").replace(/\s+/g, " ").trim();
    }
    function visible(el) {
      return Boolean(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    }
    function labelFor(el) {
      if (!el.id) {
        return "";
      }
      return clean(document.querySelector(`label[for="${CSS.escape(el.id)}"]`)?.innerText || "");
    }
    function valueOf(id) {
      return document.getElementById(id)?.value || "";
    }
    function checked(id) {
      return Boolean(document.getElementById(id)?.checked);
    }
    function selectedText(id) {
      const select = document.getElementById(id);
      return clean(select?.selectedOptions?.[0]?.text || "");
    }
    const controls = Array.from(document.querySelectorAll("input,select,textarea")).filter(visible).map((el) => {
      const item = {
        id: el.id || "",
        label: labelFor(el),
        name: el.name || "",
        tag: el.tagName.toLowerCase(),
        type: el.tagName.toLowerCase() === "select" ? "select" : (el.type || ""),
        value: el.value || "",
        visible: visible(el),
      };
      if (el.tagName.toLowerCase() === "select") {
        item.selected = Array.from(el.selectedOptions || []).map((option) => ({
          text: option.text,
          value: option.value,
        }));
      }
      if (el.type === "checkbox" || el.type === "radio") {
        item.checked = Boolean(el.checked);
      }
      return item;
    });

    let visibility = "";
    if (checked("ctl00_visibility_everyone_radiobutton")) {
      visibility = "To Everyone";
    } else if (checked("ctl00_visibility_members_radiobutton")) {
      visibility = "To Members Only";
    } else if (checked("ctl00_visibility_hidden_radiobutton")) {
      visibility = "Hidden";
    }

    return {
      bodyTextStart: clean(document.body?.innerText || "").slice(0, 2000),
      controls,
      fields: {
        activeDate: valueOf("ctl00_active_date_date_text_box"),
        author: valueOf("ctl00_author_name"),
        category: selectedText("ctl00_category_dropdown"),
        categoryValue: valueOf("ctl00_category_dropdown"),
        expires: valueOf("ctl00_expiration_date_date_text_box"),
        headline: valueOf("ctl00_headline_text"),
        showActiveDateOnStory: checked("ctl00_show_active_date_checkbox"),
        showAuthorOnStory: checked("ctl00_show_author_checkbox"),
        showOnMainNewsPage: checked("ctl00_show_on_news_page_checkbox"),
        summary: valueOf("ctl00_summary_text"),
        tagsInput: valueOf("ctl00_tags_tag_box_Input"),
        visibility,
      },
      title: document.title,
      url: location.href,
    };
  }, undefined, { timeoutMs: 10000 });
}

async function extractEditorSource(tab, url) {
  await tab.goto(url);
  await tab.playwright.waitForLoadState({ state: "domcontentloaded", timeoutMs: 30000 });
  return await tab.playwright.evaluate(() => {
    function clean(value) {
      return (value || "").replace(/\s+/g, " ").trim();
    }
    function normalizeUrl(value) {
      if (!value) {
        return "";
      }
      if (value.startsWith("//")) {
        return `https:${value}`;
      }
      return value;
    }
    function decodeEditorValue(value) {
      if (!value) {
        return "";
      }
      try {
        return decodeURIComponent(value.replace(/\+/g, "%20"));
      } catch {
        return value;
      }
    }
    const candidates = Array.from(document.querySelectorAll("textarea,input[type='hidden']")).map((el) => {
      const raw = el.value || "";
      const decoded = decodeEditorValue(raw);
      const score = decoded.length + (/<[a-z][\s\S]*>/i.test(decoded) ? 100000 : 0);
      return { raw, decoded, score };
    }).filter((candidate) => candidate.raw && (candidate.raw.includes("%3c") || candidate.raw.includes("%3C") || /<[a-z][\s\S]*>/i.test(candidate.decoded)));
    candidates.sort((a, b) => b.score - a.score);
    const selected = candidates[0] || { raw: "", decoded: "", score: 0 };
    const imageSrcs = extractTagAttributes(selected.decoded, "img", "src").map(normalizeUrl);
    const links = extractLinks(selected.decoded);
    return {
      raw: selected.raw,
      html: selected.decoded,
      metadata: {
        url: location.href,
        title: document.title,
        bodyTextStart: clean(document.body?.innerText || "").slice(0, 2000),
        rawEditorFieldFound: Boolean(selected.raw),
        rawEditorValueLength: selected.raw.length,
        decodedHtmlLength: selected.decoded.length,
        imageSrcs,
        links,
      },
    };

    function extractTagAttributes(html, tagName, attrName) {
      const values = [];
      const tagPattern = new RegExp(`<${tagName}\\b[^>]*>`, "gi");
      const attrPattern = new RegExp(`${attrName}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, "i");
      for (const tag of html.match(tagPattern) || []) {
        const match = tag.match(attrPattern);
        if (match) {
          values.push(match[1] || match[2] || match[3] || "");
        }
      }
      return values;
    }

    function extractLinks(html) {
      const links = [];
      const linkPattern = /<a\b[^>]*href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))[^>]*>([\s\S]*?)<\/a>/gi;
      let match;
      while ((match = linkPattern.exec(html))) {
        links.push({
          href: match[1] || match[2] || match[3] || "",
          text: clean(stripTags(match[4] || "")),
        });
      }
      return links;
    }

    function stripTags(html) {
      return html.replace(/<[^>]+>/g, " ").replace(/&nbsp;/gi, " ");
    }
  }, undefined, { timeoutMs: 10000 });
}

async function extractRenderedArticle(tab, url, itemId) {
  await tab.goto(url);
  await tab.playwright.waitForLoadState({ state: "domcontentloaded", timeoutMs: 30000 });
  await tab.playwright.waitForLoadState({ state: "load", timeoutMs: 30000 }).catch(() => {});
  return await tab.playwright.evaluate((arg) => {
    function clean(value) {
      return (value || "").replace(/\s+/g, " ").trim();
    }
    function normalizeUrl(value) {
      if (!value) {
        return "";
      }
      if (value.startsWith("//")) {
        return `https:${value}`;
      }
      try {
        return new URL(value, location.href).href;
      } catch {
        return value;
      }
    }
    const newsText = document.querySelector(".news-text");
    const headlineEl = document.querySelector("h2.zero-top-margin") || document.querySelector("h1");
    const container = newsText?.parentElement || headlineEl?.parentElement || document.querySelector("#page_content") || document.body;
    const articleImages = Array.from((newsText || container).querySelectorAll("img[src]")).map((img) => ({
      alt: img.getAttribute("alt") || "",
      height: img.naturalHeight || img.height || numberAttr(img.getAttribute("height")),
      rawSrc: img.getAttribute("src") || "",
      src: normalizeUrl(img.getAttribute("src") || ""),
      title: img.getAttribute("title") || "",
      width: img.naturalWidth || img.width || numberAttr(img.getAttribute("width")),
    }));
    const articleLinks = Array.from((newsText || container).querySelectorAll("a[href]")).map((link) => ({
      href: link.href || normalizeUrl(link.getAttribute("href") || ""),
      text: clean(link.textContent || ""),
    }));
    return {
      articleHtml: newsText?.innerHTML || "",
      containerHtml: container?.innerHTML || "",
      metadata: {
        articleImages,
        articleLinks,
        articleText: clean(newsText?.innerText || ""),
        headline: clean(headlineEl?.innerText || ""),
        itemId: arg.itemId,
        publishedDateText: clean(document.querySelector("#ctl00_ctl00_date_panel")?.innerText || ""),
        title: document.title,
        url: location.href,
      },
    };

    function numberAttr(value) {
      const parsed = Number.parseInt(value || "", 10);
      return Number.isFinite(parsed) ? parsed : 0;
    }
  }, { itemId }, { timeoutMs: 10000 });
}

async function captureArticleAssets(tab, articleDir, articleImages) {
  const requestedArticleImageUrls = (articleImages || []).map((image) => normalizeUrl(image.src || image.rawSrc)).filter(Boolean);
  const managedImageUrls = requestedArticleImageUrls.filter(isManagedImageUrl);
  const externalImageUrlsNotFetched = requestedArticleImageUrls.filter((url) => !isManagedImageUrl(url));
  const captured = [];
  const failures = [];
  let inventorySummary = null;

  if (managedImageUrls.length) {
    const assetsDir = path.join(articleDir, "assets");
    await mkdir(assetsDir, { recursive: true });
    const capability = await tab.capabilities.get("pageAssets");
    const inventory = await capability.list();
    inventorySummary = inventory.summary;
    const requestedSet = new Set(managedImageUrls.map(normalizeComparableUrl));
    const imageAssets = (inventory.assets || []).filter((asset) => {
      if (asset.kind !== "image") {
        return false;
      }
      const comparable = normalizeComparableUrl(asset.url);
      return requestedSet.has(comparable) || requestedSet.has(s3ToImagesUrl(comparable)) || requestedSet.has(imagesToS3Url(comparable));
    });
    if (imageAssets.length) {
      const bundled = await capability.bundle({
        inventoryId: inventory.id,
        assetIds: imageAssets.map((asset) => asset.id),
      });
      const bundleByUrl = new Map((bundled.assets || []).map((asset) => [normalizeComparableUrl(asset.url), asset]));
      for (const sourceUrl of managedImageUrls) {
        const comparable = normalizeComparableUrl(sourceUrl);
        const asset = bundleByUrl.get(comparable) || bundleByUrl.get(s3ToImagesUrl(comparable)) || bundleByUrl.get(imagesToS3Url(comparable));
        if (!asset) {
          failures.push({
            sourceUrl,
            error: "Managed image was present in article markup but not bundled from pageAssets.",
          });
          continue;
        }
        const digest = await fileSha256(asset.path);
        const name = safeFileName(asset.name || path.basename(new URL(asset.url).pathname) || "asset");
        const relativeFile = `assets/${digest.slice(0, 16)}-${name}`;
        const targetPath = path.join(articleDir, relativeFile);
        await copyFile(asset.path, targetPath);
        const info = await stat(targetPath);
        captured.push({
          sourceUrl,
          contentType: asset.contentType,
          kind: asset.kind,
          originalName: name,
          file: relativeFile,
          byteLength: info.size,
          sha256: digest,
          captureMethod: "pageAssets",
        });
      }
      for (const failure of bundled.failures || []) {
        failures.push({
          sourceUrl: failure.url,
          originalName: failure.name,
          contentType: failure.contentType,
          error: failure.reason,
        });
      }
    } else {
      for (const sourceUrl of managedImageUrls) {
        failures.push({
          sourceUrl,
          error: "Managed image was present in article markup but not visible in pageAssets inventory.",
        });
      }
    }
  }

  return {
    requestedArticleImageUrls,
    managedImageUrls,
    externalImageUrlsNotFetched,
    captured,
    failures,
    inventorySummary,
  };
}

function buildArticleSummary({ row, configure, editor, rendered, assetManifest, ordinal }) {
  return {
    ordinal,
    itemId: row.itemId,
    headline: row.headline,
    status: row.status,
    activeDate: row.activeDate,
    author: row.author,
    folder: row.folder || row.itemId,
    configureFieldsCaptured: Object.keys(configure.fields || {}).length,
    visibility: configure.fields?.visibility || "",
    showOnMainNewsPage: Boolean(configure.fields?.showOnMainNewsPage),
    editorHtmlLength: editor.decodedHtmlLength || 0,
    editorImageCount: (editor.imageSrcs || []).length,
    renderedTextLength: (rendered.articleText || "").length,
    renderedArticleImageCount: (rendered.articleImages || []).length,
    renderedArticleLinkCount: (rendered.articleLinks || []).length,
    managedImageCount: (assetManifest.managedImageUrls || []).length,
    externalImageCount: (assetManifest.externalImageUrlsNotFetched || []).length,
    assetCapturedCount: (assetManifest.captured || []).length,
    assetFailureCount: (assetManifest.failures || []).length,
  };
}

async function buildSkipIds(previousSampleDirs, extraSkipIds) {
  const ids = new Set((extraSkipIds || []).map(String));
  for (const previousSampleDir of previousSampleDirs || []) {
    if (!previousSampleDir) {
      continue;
    }
    const manifest = await readJsonIfExists(path.join(previousSampleDir, "sample-manifest.json"));
    for (const article of manifest?.articles || []) {
      if (article.itemId) {
        ids.add(String(article.itemId));
      }
      if (article.folder) {
        ids.add(String(article.folder));
      }
    }
    for (const row of manifest?.rowSelection?.skippedRows || []) {
      if (row.itemId) {
        ids.add(String(row.itemId));
      }
    }
    try {
      const entries = await readdir(previousSampleDir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isDirectory() && /^\d+$/.test(entry.name)) {
          ids.add(entry.name);
        }
      }
    } catch {
      // The manifest is enough when a prior directory is not available.
    }
  }
  return ids;
}

function dateKeyFromIsoDate(value) {
  if (!value) {
    return null;
  }
  const match = String(value).trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    throw new Error(`Expected ISO date YYYY-MM-DD, got ${value}`);
  }
  return Number(`${match[1]}${match[2]}${match[3]}`);
}

function dateKeyFromClubExpressDate(value) {
  const match = String(value || "").trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) {
    return null;
  }
  const month = match[1].padStart(2, "0");
  const day = match[2].padStart(2, "0");
  return Number(`${match[3]}${month}${day}`);
}

function isManagerRowVisible(row) {
  const raw = String(row.visibleCellRaw || row.visible || "").toLowerCase();
  if (!raw) {
    return true;
  }
  if (/\/images\/x\.gif\b|\\bimages\\x\.gif\b|x\.gif\b/i.test(raw)) {
    return false;
  }
  if (/\/images\/check\.gif\b|\\bimages\\check\.gif\b|alt=["']visible["']/i.test(raw)) {
    return true;
  }
  if (/\b(not visible|hidden|invisible|not published|private|members only|no|false|inactive)\b/i.test(raw)) {
    return false;
  }
  if (/<img\b/i.test(raw)) {
    return false;
  }
  return true;
}

async function writeManifest(outputDir, manifest) {
  await writeJson(path.join(outputDir, "sample-manifest.json"), manifest);
}

async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await readFile(filePath, "utf8"));
  } catch {
    return null;
  }
}

async function fileSha256(filePath) {
  const data = await readFile(filePath);
  return createHash("sha256").update(data).digest("hex");
}

function normalizeUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  if (text.startsWith("//")) {
    return `https:${text}`;
  }
  return text;
}

function normalizeComparableUrl(value) {
  return normalizeUrl(value).replace(/^http:\/\//i, "https://");
}

function isManagedImageUrl(value) {
  const url = normalizeUrl(value);
  return /^https:\/\/images\.clubexpress\.com\/454497\/graphics\//i.test(url)
    || /^https:\/\/s3\.amazonaws\.com\/ClubExpressClubFiles\/454497\/graphics\//i.test(url);
}

function s3ToImagesUrl(value) {
  return value.replace(
    /^https:\/\/s3\.amazonaws\.com\/ClubExpressClubFiles\/454497\/graphics\//i,
    "https://images.clubexpress.com/454497/graphics/",
  );
}

function imagesToS3Url(value) {
  return value.replace(
    /^https:\/\/images\.clubexpress\.com\/454497\/graphics\//i,
    "https://s3.amazonaws.com/ClubExpressClubFiles/454497/graphics/",
  );
}

function safeFileName(value) {
  return String(value || "asset").replace(/[<>:"/\\|?*\x00-\x1f]+/g, "_").replace(/\s+/g, "_").slice(0, 180);
}
