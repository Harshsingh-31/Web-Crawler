package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A multithreaded implementation of {@link WebCrawler} that leverages a
 * {@link ForkJoinPool} to crawl multiple web pages concurrently.
 */
final class ParallelWebCrawler implements WebCrawler {

  private final Clock clock;
  private final Duration timeout;
  private final int maxWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;
  private final int maxDepth;
  private final List<Pattern> excludedPatterns;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          PageParserFactory parserFactory,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls) {

    this.clock = clock;
    this.timeout = timeout;
    this.maxWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactory = parserFactory;
    this.maxDepth = maxDepth;
    this.excludedPatterns = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentMap<String, Integer> wordFrequencies = new ConcurrentHashMap<>();
    Set<String> visitedUrls = ConcurrentHashMap.newKeySet();

    // Initialize tasks for provided starting points
    List<CrawlTask> rootTasks = startingUrls.stream()
            .map(url -> new CrawlTask(url, deadline, maxDepth, wordFrequencies, visitedUrls))
            .collect(Collectors.toList());

    rootTasks.forEach(pool::invoke);

    Map<String, Integer> finalWordCounts = wordFrequencies.isEmpty()
            ? wordFrequencies
            : WordCounts.sort(wordFrequencies, maxWordCount);

    return new CrawlResult.Builder()
            .setWordCounts(finalWordCounts)
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * Task representing a single crawl operation on one URL.
   */
  private final class CrawlTask extends RecursiveAction {
    private final String url;
    private final Instant deadline;
    private final int depthRemaining;
    private final ConcurrentMap<String, Integer> wordFrequencies;
    private final Set<String> visitedUrls;

    CrawlTask(String url,
              Instant deadline,
              int depthRemaining,
              ConcurrentMap<String, Integer> wordFrequencies,
              Set<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.depthRemaining = depthRemaining;
      this.wordFrequencies = wordFrequencies;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected void compute() {
      if (depthRemaining <= 0 || clock.instant().isAfter(deadline)) {
        return;
      }
      for (Pattern pattern : excludedPatterns) {
        if (pattern.matcher(url).matches()) {
          return;
        }
      }
      if (!visitedUrls.add(url)) {
        return;
      }

      PageParser.Result result = parserFactory.get(url).parse();


      result.getWordCounts()
              .forEach((word, count) -> wordFrequencies.merge(word, count, Integer::sum));

      List<CrawlTask> subTasks = result.getLinks().stream()
              .map(link -> new CrawlTask(link, deadline, depthRemaining - 1, wordFrequencies, visitedUrls))
              .collect(Collectors.toList());

      invokeAll(subTasks);
    }
  }
}
