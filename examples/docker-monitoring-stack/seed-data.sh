#!/bin/bash
# Seed data generator for Chronicle + Grafana monitoring stack
# Generates realistic IoT sensor data: temperature, humidity, pressure
# Usage: bash seed-data.sh

CHRONICLE_URL="http://chronicle:8086"
SENSOR_IDS=("sensor-01" "sensor-02" "sensor-03" "sensor-04" "sensor-05")
DURATION_MINUTES=${1:-5}  # Default: 5 minutes of data
INTERVAL_SECONDS=${2:-10} # Default: every 10 seconds

echo "🌱 Seeding Chronicle with IoT sensor data..."
echo "   Sensors: ${SENSOR_IDS[*]}"
echo "   Duration: ${DURATION_MINUTES} minutes"
echo "   Interval: ${INTERVAL_SECONDS}s"
echo ""

TOTAL_POINTS=$(( (DURATION_MINUTES * 60) / INTERVAL_SECONDS ))
SENSORS=${#SENSOR_IDS[@]}
TOTAL=$(( TOTAL_POINTS * SENSORS ))

for i in $(seq 1 $TOTAL_POINTS); do
    TIMESTAMP=$(date +%s)
    
    for sensor in "${SENSOR_IDS[@]}"; do
        # Generate realistic sensor readings
        BASE_TEMP=22
        TEMP_OFFSET=$(awk "BEGIN{srand($RANDOM); printf \"%.1f\", ($BASE_TEMP - 5 + rand()*10) - $BASE_TEMP}")
        TEMP=$(awk "BEGIN{printf \"%.1f\", $BASE_TEMP + $TEMP_OFFSET}")
        
        BASE_HUMID=55
        HUMID_OFFSET=$(awk "BEGIN{srand($RANDOM); printf \"%.1f\", ($BASE_HUMID - 10 + rand()*20) - $BASE_HUMID}")
        HUMID=$(awk "BEGIN{printf \"%.1f\", $BASE_HUMID + $HUMID_OFFSET}")
        
        BASE_PRESSURE=1013
        PRESSURE_OFFSET=$(awk "BEGIN{srand($RANDOM); printf \"%.1f\", ($BASE_PRESSURE - 5 + rand()*10) - $BASE_PRESSURE}")
        PRESSURE=$(awk "BEGIN{printf \"%.1f\", $BASE_PRESSURE + $PRESSURE_OFFSET}")
        
        # Write to Chronicle (Prometheus remote write format)
        curl -s -X POST "${CHRONICLE_URL}/write" \
          -d "temperature,${sensor}=live value=${TEMP},${TIMESTAMP}" \
          -H "Content-Type: text/plain" 2>/dev/null
        
        curl -s -X POST "${CHRONICLE_URL}/write" \
          -d "humidity,${sensor}=live value=${HUMID},${TIMESTAMP}" \
          -H "Content-Type: text/plain" 2>/dev/null
        
        curl -s -X POST "${CHRONICLE_URL}/write" \
          -d "pressure,${sensor}=live value=${PRESSURE},${TIMESTAMP}" \
          -H "Content-Type: text/plain" 2>/dev/null
    done
    
    # Progress indicator
    if [ $((i % 10)) -eq 0 ]; then
        PROGRESS=$((i * 100 / TOTAL_POINTS))
        echo -ne "  Progress: ${PROGRESS}% (${i}/${TOTAL_POINTS}) points\r"
    fi
    
    sleep $INTERVAL_SECONDS
done

echo ""
echo "✅ Seeding complete! Total points: $TOTAL"
echo ""
echo "Next steps:"
echo "  1. Open Grafana: http://localhost:3000 (admin / chronicle)"
echo "  2. Import dashboard: Chronicle IoT Monitoring"
echo "  3. Query sample: avg(temperature{sensor=~\"$sensor\"})"
