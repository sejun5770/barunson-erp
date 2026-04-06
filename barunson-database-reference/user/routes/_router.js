// routes/_router.js — 간단한 라우터 시스템
// 각 모듈이 등록한 라우트를 순서대로 매칭

class Router {
  constructor() {
    this.routes = [];       // { method, pattern, handler, name }
    this.paramRoutes = [];  // regex-based routes with params
  }

  // 정적 라우트 등록
  add(method, path, handler) {
    this.routes.push({ method: method.toUpperCase(), path, handler });
  }

  // 파라미터 라우트 등록 (regex)
  addPattern(method, regex, handler) {
    this.paramRoutes.push({ method: method.toUpperCase(), regex, handler });
  }

  // GET/POST/PUT/DELETE 단축
  get(path, handler) { this.add('GET', path, handler); }
  post(path, handler) { this.add('POST', path, handler); }
  put(path, handler) { this.add('PUT', path, handler); }
  del(path, handler) { this.add('DELETE', path, handler); }
  patch(path, handler) { this.add('PATCH', path, handler); }

  // 패턴 단축
  getP(regex, handler) { this.addPattern('GET', regex, handler); }
  postP(regex, handler) { this.addPattern('POST', regex, handler); }
  putP(regex, handler) { this.addPattern('PUT', regex, handler); }
  delP(regex, handler) { this.addPattern('DELETE', regex, handler); }

  // 라우트 매칭
  async handle(req, res, pathname, method, parsed) {
    // 정적 라우트 먼저
    for (const r of this.routes) {
      if (r.method === method && r.path === pathname) {
        await r.handler(req, res, parsed);
        return true;
      }
    }
    // 파라미터 라우트
    for (const r of this.paramRoutes) {
      if (r.method === method) {
        const m = pathname.match(r.regex);
        if (m) {
          await r.handler(req, res, parsed, m);
          return true;
        }
      }
    }
    return false;
  }

  // 라우트 수 반환
  get count() {
    return this.routes.length + this.paramRoutes.length;
  }
}

module.exports = Router;
