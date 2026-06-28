package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Port                       int
	TargetURL                  string
	ProxySourceURL             string
	ProxySourcePages           int
	ProxySourcePageSize        int
	ProxySourceMaxPages        int
	ProxyRequireHTTPS          bool
	ProxyRefresh               time.Duration
	ProxyTimeout               time.Duration
	ProxyAttemptTimeout        time.Duration
	ProxyValidationConcurrency int
	HealthyProxyTarget         int
	HealthyProxyMin            int
	MaxProxyFailures           int
	MaxProxyAttempts           int
	TargetHeaders              http.Header
}

type Proxy struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

type HealthyProxy struct {
	Proxy
	Latency   time.Duration
	Failures  int
	Successes int
	LastOK    time.Time
	LastFail  time.Time
	LastError string
}

type ProxyPool struct {
	cfg            Config
	mu             sync.Mutex
	proxies        []Proxy
	candidates     []Proxy
	dead           map[string]struct{}
	healthy        []*HealthyProxy
	candidateIndex int
	healthyIndex   int
	lastRefresh    time.Time
	refreshing     bool
	maintaining    bool
}

func main() {
	cfg := loadConfig()
	pool := NewProxyPool(cfg)
	pool.StartMaintenance()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleRequest(w, r, cfg, pool)
		}),
	}

	log.Printf("Proxy rotativo ouvindo em http://localhost:%d", cfg.Port)
	log.Printf("Destino configurado: %s", cfg.TargetURL)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request, cfg Config, pool *ProxyPool) {
	if r.URL.Path == "/health" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stats": pool.Stats()})
		return
	}

	if r.URL.Path == "/refresh" {
		ctx, cancel := context.WithTimeout(r.Context(), cfg.ProxyTimeout*3)
		defer cancel()

		if err := pool.Refresh(ctx, true); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}

		pool.MaintainAsync("refresh manual")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stats": pool.Stats()})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	targetURL, err := buildTargetURL(cfg.TargetURL, r.URL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	resp, proxy, err := requestWithRotation(r.Context(), cfg, pool, targetURL, r.Method, body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error(), "stats": pool.Stats()})
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set("X-Farias-Upstream-Proxy", fmt.Sprintf("%s://%s:%d", proxy.Protocol, proxy.Host, proxy.Port))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func requestWithRotation(parent context.Context, cfg Config, pool *ProxyPool, targetURL string, method string, body []byte) (*http.Response, Proxy, error) {
	ctx, cancel := context.WithTimeout(parent, cfg.ProxyTimeout)
	defer cancel()

	if err := pool.EnsureMinimumHealthy(ctx); err != nil {
		return nil, Proxy{}, err
	}

	var lastErr error
	tried := make(map[string]struct{})
	maxAttempts := min(cfg.MaxProxyAttempts, max(1, pool.HealthyCount()))

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		proxy, err := pool.NextHealthyExcept(ctx, tried)
		if err != nil {
			lastErr = err
			break
		}
		tried[proxyKey(proxy)] = struct{}{}

		started := time.Now()
		resp, err := DoThroughProxy(ctx, cfg, proxy, targetURL, method, body)
		if err == nil {
			pool.MarkSuccess(proxy, time.Since(started))
			return resp, proxy, nil
		}

		lastErr = err
		pool.MarkFailure(proxy, err)
		log.Printf("Proxy saudavel falhou %d/%d via %s:%d; tentando proximo saudavel: %s", attempt, maxAttempts, proxy.Host, proxy.Port, err.Error())
	}

	return nil, Proxy{}, fmt.Errorf("nenhum proxy saudavel funcionou dentro de %s apos %d tentativa(s); pool=%+v; ultimo erro: %v", cfg.ProxyTimeout, len(tried), pool.Stats(), lastErr)
}

func NewProxyPool(cfg Config) *ProxyPool {
	return &ProxyPool{cfg: cfg, dead: make(map[string]struct{})}
}

func (p *ProxyPool) StartMaintenance() {
	p.MaintainAsync("bootstrap inicial")

	interval := p.cfg.ProxyRefresh / 3
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			p.MaintainAsync("manutencao periodica")
		}
	}()
}

func (p *ProxyPool) MaintainAsync(reason string) {
	go func() {
		log.Printf("Iniciando busca e validacao de proxies: %s", reason)

		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProxyTimeout*time.Duration(max(2, p.cfg.HealthyProxyTarget)))
		defer cancel()

		if err := p.Maintain(ctx); err != nil {
			log.Printf("Manutencao do pool falhou: %s", err.Error())
			return
		}

		log.Printf("Busca e validacao finalizadas: %+v", p.Stats())
	}()
}

func (p *ProxyPool) Refresh(ctx context.Context, force bool) error {
	p.mu.Lock()
	shouldRefresh := force || time.Since(p.lastRefresh) >= p.cfg.ProxyRefresh || len(p.proxies) == 0
	if !shouldRefresh {
		p.mu.Unlock()
		return nil
	}
	if p.refreshing {
		p.mu.Unlock()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				p.mu.Lock()
				refreshing := p.refreshing
				p.mu.Unlock()
				if !refreshing {
					return nil
				}
			}
		}
	}
	p.refreshing = true
	p.mu.Unlock()

	proxies, err := FetchProxyList(ctx, p.cfg)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.refreshing = false

	if err != nil {
		return err
	}
	if len(proxies) == 0 {
		return errors.New("nenhum proxy encontrado na fonte configurada")
	}

	added := p.mergeCandidates(proxies)
	p.lastRefresh = time.Now()
	log.Printf("Fonte de proxies atualizada: %d coletados, %d novos candidatos, total=%d, candidatos_restantes=%d", len(proxies), added, len(p.proxies), max(0, len(p.candidates)-p.candidateIndex))
	return nil
}

func (p *ProxyPool) Maintain(ctx context.Context) error {
	p.mu.Lock()
	if p.maintaining {
		p.mu.Unlock()
		return nil
	}
	p.maintaining = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.maintaining = false
		p.mu.Unlock()
	}()

	if err := p.Refresh(ctx, false); err != nil {
		return err
	}

	if p.HealthyCount() >= p.cfg.HealthyProxyTarget {
		return nil
	}

	jobs := make(chan Proxy)
	var wg sync.WaitGroup
	var checked int64
	var checkedMu sync.Mutex

	for i := 0; i < p.cfg.ProxyValidationConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for proxy := range jobs {
				if p.HealthyCount() >= p.cfg.HealthyProxyTarget {
					continue
				}

				checkedMu.Lock()
				checked++
				checkedMu.Unlock()

				p.validateCandidate(ctx, proxy)
			}
		}()
	}

sendLoop:
	for p.HealthyCount() < p.cfg.HealthyProxyTarget {
		proxy, ok := p.nextCandidate()
		if !ok {
			break
		}

		select {
		case <-ctx.Done():
			break sendLoop
		case jobs <- proxy:
		}
	}

	close(jobs)
	wg.Wait()

	p.sortHealthy()
	log.Printf("Manutencao do pool: %d/%d saudaveis, %d candidatos restantes, %d descartados", p.HealthyCount(), p.cfg.HealthyProxyTarget, p.CandidateCount(), p.DeadCount())
	return ctx.Err()
}

func (p *ProxyPool) EnsureMinimumHealthy(ctx context.Context) error {
	if p.HealthyCount() >= p.cfg.HealthyProxyMin {
		return nil
	}

	if err := p.Maintain(ctx); err != nil && p.HealthyCount() == 0 {
		return err
	}

	if p.HealthyCount() == 0 {
		return fmt.Errorf("nenhum proxy saudavel disponivel; stats=%+v", p.Stats())
	}

	return nil
}

func (p *ProxyPool) NextHealthy(ctx context.Context) (Proxy, error) {
	return p.NextHealthyExcept(ctx, nil)
}

func (p *ProxyPool) NextHealthyExcept(ctx context.Context, exclude map[string]struct{}) (Proxy, error) {
	if err := p.EnsureMinimumHealthy(ctx); err != nil {
		return Proxy{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.sortHealthyLocked()
	for checked := 0; checked < len(p.healthy); checked++ {
		item := p.healthy[p.healthyIndex%len(p.healthy)]
		p.healthyIndex++

		if _, skip := exclude[proxyKey(item.Proxy)]; skip {
			continue
		}

		return item.Proxy, nil
	}

	return Proxy{}, errors.New("todos os proxies saudaveis disponiveis ja foram tentados nesta requisicao")
}

func (p *ProxyPool) MarkSuccess(proxy Proxy, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	item := p.findHealthyLocked(proxy)
	if item == nil {
		return
	}

	item.Latency = (item.Latency*7 + latency*3) / 10
	item.Failures = 0
	item.Successes++
	item.LastOK = time.Now()
}

func (p *ProxyPool) MarkFailure(proxy Proxy, err error) {
	p.mu.Lock()
	item := p.findHealthyLocked(proxy)
	if item == nil {
		p.mu.Unlock()
		return
	}

	item.Failures++
	item.LastFail = time.Now()
	item.LastError = err.Error()
	shouldRemove := item.Failures >= p.cfg.MaxProxyFailures
	p.mu.Unlock()

	if shouldRemove {
		p.removeHealthy(proxy)
		log.Printf("Proxy descartado: %s:%d apos %d falhas", proxy.Host, proxy.Port, p.cfg.MaxProxyFailures)
	}

	if p.HealthyCount() < p.cfg.HealthyProxyMin {
		p.MaintainAsync("reposicao por pool abaixo do minimo")
	}
}

func (p *ProxyPool) Stats() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return map[string]any{
		"proxies":              len(p.proxies),
		"candidates":           max(0, len(p.candidates)-p.candidateIndex),
		"healthyProxies":       len(p.healthy),
		"deadProxies":          len(p.dead),
		"targetHealthyProxies": p.cfg.HealthyProxyTarget,
		"minHealthyProxies":    p.cfg.HealthyProxyMin,
	}
}

func (p *ProxyPool) HealthyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.healthy)
}

func (p *ProxyPool) CandidateCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return max(0, len(p.candidates)-p.candidateIndex)
}

func (p *ProxyPool) DeadCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.dead)
}

func (p *ProxyPool) validateCandidate(ctx context.Context, proxy Proxy) {
	started := time.Now()
	resp, err := DoThroughProxy(ctx, p.cfg, proxy, p.cfg.TargetURL, http.MethodGet, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		p.mu.Lock()
		p.dead[proxyKey(proxy)] = struct{}{}
		p.mu.Unlock()
		return
	}

	p.addHealthy(proxy, time.Since(started))
}

func (p *ProxyPool) mergeCandidates(proxies []Proxy) int {
	known := make(map[string]struct{}, len(p.proxies)+len(p.healthy)+len(p.dead))
	for _, proxy := range p.proxies {
		known[proxyKey(proxy)] = struct{}{}
	}
	for _, proxy := range p.healthy {
		known[proxyKey(proxy.Proxy)] = struct{}{}
	}
	// Proxies descartados podem voltar a ficar validos em outra coleta; permitir
	// reentrada evita esgotar a fonte quando a API retorna poucos itens por pagina.

	added := 0
	for _, proxy := range proxies {
		key := proxyKey(proxy)
		if _, ok := known[key]; ok {
			continue
		}
		known[key] = struct{}{}
		delete(p.dead, key)
		p.proxies = append(p.proxies, proxy)
		p.candidates = append(p.candidates, proxy)
		added++
	}

	return added
}

func (p *ProxyPool) nextCandidate() (Proxy, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.candidateIndex < len(p.candidates) {
		proxy := p.candidates[p.candidateIndex]
		p.candidateIndex++

		if _, dead := p.dead[proxyKey(proxy)]; dead {
			continue
		}
		if p.findHealthyLocked(proxy) != nil {
			continue
		}

		return proxy, true
	}

	return Proxy{}, false
}

func (p *ProxyPool) addHealthy(proxy Proxy, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.findHealthyLocked(proxy) != nil {
		return
	}

	p.healthy = append(p.healthy, &HealthyProxy{
		Proxy:     proxy,
		Latency:   latency,
		Failures:  0,
		Successes: 1,
		LastOK:    time.Now(),
	})

	log.Printf("Proxy saudavel adicionado: %s:%d em %s (%d/%d)", proxy.Host, proxy.Port, latency.Round(time.Millisecond), len(p.healthy), p.cfg.HealthyProxyTarget)
}

func (p *ProxyPool) removeHealthy(proxy Proxy) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := proxyKey(proxy)
	p.dead[key] = struct{}{}
	filtered := p.healthy[:0]
	for _, item := range p.healthy {
		if proxyKey(item.Proxy) != key {
			filtered = append(filtered, item)
		}
	}
	p.healthy = filtered
	p.healthyIndex = 0
}

func (p *ProxyPool) findHealthyLocked(proxy Proxy) *HealthyProxy {
	key := proxyKey(proxy)
	for _, item := range p.healthy {
		if proxyKey(item.Proxy) == key {
			return item
		}
	}
	return nil
}

func (p *ProxyPool) sortHealthy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sortHealthyLocked()
}

func (p *ProxyPool) sortHealthyLocked() {
	sort.SliceStable(p.healthy, func(i, j int) bool {
		return scoreProxy(p.healthy[i]) < scoreProxy(p.healthy[j])
	})
}

func FetchProxyList(ctx context.Context, cfg Config) ([]Proxy, error) {
	client := &http.Client{Timeout: cfg.ProxyTimeout}
	type result struct {
		proxies []Proxy
		err     error
	}

	pages := cfg.ProxySourcePages
	if pages <= 0 {
		pages = 1
	}
	results := make(chan result, pages)
	var wg sync.WaitGroup

	for page := 1; page <= pages; page++ {
		pageURL := withPage(cfg.ProxySourceURL, page, cfg.ProxySourcePageSize)
		wg.Add(1)
		go func() {
			defer wg.Done()
			proxies, err := fetchProxyPage(ctx, client, pageURL, cfg.ProxyRequireHTTPS)
			results <- result{proxies: proxies, err: err}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]struct{})
	var proxies []Proxy
	var lastErr error

	for result := range results {
		if result.err != nil {
			lastErr = result.err
			continue
		}

		for _, proxy := range result.proxies {
			key := proxyKey(proxy)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			proxies = append(proxies, proxy)
		}
	}

	if len(proxies) < cfg.HealthyProxyTarget && cfg.ProxySourceMaxPages > pages {
		more, err := fetchProxyListSequential(ctx, client, cfg, pages+1, cfg.ProxySourceMaxPages, seen)
		if err != nil {
			lastErr = err
		}
		proxies = append(proxies, more...)
	}

	if len(proxies) == 0 && lastErr != nil {
		return nil, lastErr
	}

	return proxies, nil
}

func fetchProxyListSequential(ctx context.Context, client *http.Client, cfg Config, startPage int, maxPage int, seen map[string]struct{}) ([]Proxy, error) {
	var proxies []Proxy
	var lastErr error

	for page := startPage; page <= maxPage && len(proxies) < cfg.HealthyProxyTarget; page++ {
		pageURL := withPage(cfg.ProxySourceURL, page, cfg.ProxySourcePageSize)
		pageProxies, err := fetchProxyPage(ctx, client, pageURL, cfg.ProxyRequireHTTPS)
		if err != nil {
			lastErr = err
			continue
		}
		if len(pageProxies) == 0 {
			break
		}

		for _, proxy := range pageProxies {
			key := proxyKey(proxy)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			proxies = append(proxies, proxy)
		}
	}

	return proxies, lastErr
}

type freeProxyDBResponse struct {
	Data struct {
		TotalCount int `json:"total_count"`
		Data       []struct {
			IP       string `json:"ip"`
			Port     int    `json:"port"`
			Protocol string `json:"protocol"`
			IsValid  int    `json:"is_valid"`
			HTTPS    int    `json:"https"`
		} `json:"data"`
	} `json:"data"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

func fetchProxyPage(ctx context.Context, client *http.Client, pageURL string, requireHTTPS bool) ([]Proxy, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("FreeProxyDB respondeu HTTP %d", resp.StatusCode)
	}

	var payload freeProxyDBResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	if payload.Status != 1 {
		return nil, fmt.Errorf("FreeProxyDB respondeu status=%d message=%q", payload.Status, payload.Message)
	}

	seen := make(map[string]struct{})
	proxies := make([]Proxy, 0, len(payload.Data.Data))
	for _, item := range payload.Data.Data {
		if item.IsValid != 1 || item.Port < 1 || item.Port > 65535 || net.ParseIP(item.IP) == nil {
			continue
		}
		if requireHTTPS && item.HTTPS != 1 {
			continue
		}

		protocol := strings.ToLower(item.Protocol)
		if protocol == "" {
			protocol = "http"
		}

		proxy := Proxy{Host: item.IP, Port: item.Port, Protocol: protocol}
		key := proxyKey(proxy)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		proxies = append(proxies, proxy)
	}

	return proxies, nil
}

func DoThroughProxy(parent context.Context, cfg Config, proxy Proxy, targetURL string, method string, body []byte) (*http.Response, error) {
	attemptTimeout := cfg.ProxyAttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = cfg.ProxyTimeout
	}

	ctx, cancel := context.WithTimeout(parent, attemptTimeout)
	defer cancel()

	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(&url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(proxy.Host, strconv.Itoa(proxy.Port)),
		}),
		DialContext: (&net.Dialer{
			Timeout:   attemptTimeout,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		TLSClientConfig:       &tls.Config{ServerName: target.Hostname(), MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   attemptTimeout,
		ResponseHeaderTimeout: attemptTimeout,
		ExpectContinueTimeout: time.Second,
		DisableKeepAlives:     true,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{Transport: transport, Timeout: attemptTimeout}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	for name, values := range cfg.TargetHeaders {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	if len(body) > 0 {
		req.ContentLength = int64(len(body))
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func loadConfig() Config {
	loadEnvFile(".env")

	return Config{
		Port:                       intEnv("PORT", 3000),
		TargetURL:                  stringEnv("TARGET_URL", "https://httpbin.org/ip"),
		ProxySourceURL:             stringEnv("PROXY_SOURCE_URL", "https://freeproxydb.com/api/proxy/search?country=&protocol=http&anonymity=elite,anonymous&speed=0,60&page_index=1&page_size=100"),
		ProxySourcePages:           intEnv("PROXY_SOURCE_PAGES", 20),
		ProxySourcePageSize:        intEnv("PROXY_SOURCE_PAGE_SIZE", 100),
		ProxySourceMaxPages:        intEnv("PROXY_SOURCE_MAX_PAGES", 100),
		ProxyRequireHTTPS:          boolEnv("PROXY_REQUIRE_HTTPS", false),
		ProxyRefresh:               time.Duration(intEnv("PROXY_REFRESH_SECONDS", 300)) * time.Second,
		ProxyTimeout:               time.Duration(intEnv("PROXY_TIMEOUT_MS", 5000)) * time.Millisecond,
		ProxyAttemptTimeout:        time.Duration(intEnv("PROXY_ATTEMPT_TIMEOUT_MS", 5000)) * time.Millisecond,
		ProxyValidationConcurrency: intEnv("PROXY_VALIDATION_CONCURRENCY", 16),
		HealthyProxyTarget:         intEnv("HEALTHY_PROXY_TARGET", 25),
		HealthyProxyMin:            intEnv("HEALTHY_PROXY_MIN", 5),
		MaxProxyFailures:           intEnv("MAX_PROXY_FAILURES", 2),
		MaxProxyAttempts:           intEnv("MAX_PROXY_ATTEMPTS", 25),
		TargetHeaders:              parseTargetHeaders(),
	}
}

func loadEnvFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)

		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

func parseTargetHeaders() http.Header {
	headers := make(http.Header)
	if auth := os.Getenv("TARGET_AUTHORIZATION"); auth != "" {
		headers.Set("Authorization", auth)
	}

	if raw := os.Getenv("TARGET_HEADERS"); raw != "" {
		for _, part := range strings.Split(raw, "|") {
			name, value, ok := strings.Cut(part, ":")
			if !ok {
				continue
			}
			name = strings.TrimSpace(name)
			value = strings.TrimSpace(value)
			if name != "" && value != "" {
				headers.Set(name, value)
			}
		}
	}

	return headers
}

func copyResponseHeaders(dst, src http.Header) {
	blocked := map[string]struct{}{
		"Connection":          {},
		"Keep-Alive":          {},
		"Proxy-Authenticate":  {},
		"Proxy-Authorization": {},
		"Te":                  {},
		"Trailer":             {},
		"Transfer-Encoding":   {},
		"Upgrade":             {},
	}

	for name, values := range src {
		if _, ok := blocked[http.CanonicalHeaderKey(name)]; ok {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func buildTargetURL(base string, incoming *url.URL) (string, error) {
	target, err := url.Parse(base)
	if err != nil {
		return "", err
	}

	if incoming.Path != "" && incoming.Path != "/" {
		target.Path = joinURLPath(target.Path, incoming.Path)
	}

	if incoming.RawQuery != "" {
		if target.RawQuery == "" {
			target.RawQuery = incoming.RawQuery
		} else {
			target.RawQuery = target.RawQuery + "&" + incoming.RawQuery
		}
	}

	return target.String(), nil
}

func joinURLPath(basePath string, extraPath string) string {
	if basePath == "" || basePath == "/" {
		return "/" + strings.TrimLeft(extraPath, "/")
	}

	return strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(extraPath, "/")
}

func withPage(rawURL string, page int, pageSize int) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	query := parsed.Query()
	query.Set("page_index", strconv.Itoa(page))
	query.Set("page_size", strconv.Itoa(pageSize))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func proxyKey(proxy Proxy) string {
	return fmt.Sprintf("%s://%s:%d", proxy.Protocol, proxy.Host, proxy.Port)
}

func scoreProxy(proxy *HealthyProxy) int64 {
	return proxy.Latency.Milliseconds() + int64(proxy.Failures*2000) - int64(min(proxy.Successes, 10)*50)
}

func stringEnv(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func intEnv(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func boolEnv(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if value == "" {
		return fallback
	}

	return value == "1" || value == "true" || value == "yes" || value == "on"
}
