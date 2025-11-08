import http from 'k6/http';
import { check, sleep } from 'k6';

// Define the load profile
export let options = {
  stages: [
    // Ramp up to 20 Virtual Users over 10 seconds
    { duration: '10s', target: 200 },
    // Stay at 20 VUs for some time
    { duration: '600s', target: 200 },
    // Ramp down to 0 VUs over 5 seconds
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    // 99% of requests must complete within 500ms
    http_req_duration: ['p(99) < 500'],
    // 99% success rate
    checks: ['rate>0.99'], 
  },
};

// Main function (what each VU executes)
export default function () {
  // Use the environment variable set in docker-compose.yml
  const url = `${__ENV.TARGET_URL}`; 
  
  // Example: simple GET request to the root path
  const res = http.get(url);

  check(res, {
    'is status 200': (r) => r.status === 200,
  });
  sleep(0.1); // Wait 0.1 seconds between requests
}