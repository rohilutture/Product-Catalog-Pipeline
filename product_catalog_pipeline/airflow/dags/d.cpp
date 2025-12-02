int solution(vector<int> centerCapacities, vector<string> dailyLog) {
    int n = centerCapacities.size();

    // package counters per center
    vector<int> processed(n, 0);

    // remaining capacity before reset
    vector<int> remaining = centerCapacities;

    // center open/closed tracking
    vector<bool> closed(n, false);

    // pointer to current center (circular)
    int cur = 0;

    for (auto &log : dailyLog) {
        if (log == "PACKAGE") {

            // find next open center
            int steps = 0;
            while (closed[cur] && steps < n) {
                cur = (cur + 1) % n;
                steps++;
            }

            // deliver a package
            processed[cur]++;
            remaining[cur]--;

            // if this center is now at 0 remaining capacity -> check if all open centers are also at 0
            if (remaining[cur] == 0) {
                bool all_zero = true;
                for (int i = 0; i < n; i++) {
                    if (!closed[i] && remaining[i] > 0) {
                        all_zero = false;
                        break;
                    }
                }

                // if all open centers hit zero → reset all open centers to full capacity
                if (all_zero) {
                    for (int i = 0; i < n; i++) {
                        if (!closed[i]) remaining[i] = centerCapacities[i];
                    }
                }
            }

            // move to next center for next package
            cur = (cur + 1) % n;
        }

        else {  
            // format: "CLOSURE X" → close center X
            int idx = stoi(log.substr(8));
            closed[idx] = true;
        }
    }

    // find center with max processed packages
    int best = 0;
    for (int i = 1; i < n; i++) {
        if (processed[i] >= processed[best]) best = i;
    }

    return best;
}
