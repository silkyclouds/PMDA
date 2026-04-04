interface MiniSparklineProps {
  data?: number[];
  width?: number;
  height?: number;
  color?: string;
  className?: string;
}

function buildPoints(data: number[], width: number, height: number): string {
  if (data.length === 0) return '';
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  return data
    .map((value, index) => {
      const x = data.length === 1 ? 0 : (index / (data.length - 1)) * width;
      const y = height - ((value - min) / range) * height;
      return `${x},${y}`;
    })
    .join(' ');
}

export function MiniSparkline({
  data = [],
  width = 120,
  height = 24,
  color = '#3B82F6',
  className,
}: MiniSparklineProps) {
  const samples = Array.isArray(data) ? data.filter((value) => Number.isFinite(value)) : [];
  if (samples.length < 2) return null;

  const points = buildPoints(samples, width, height - 2);
  const areaPoints = `${points} ${width},${height} 0,${height}`;

  return (
    <svg
      aria-hidden="true"
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      className={className}
    >
      <defs>
        <linearGradient id={`spark-${color.replace(/[^a-zA-Z0-9]/g, '')}`} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.18" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={areaPoints} fill={`url(#spark-${color.replace(/[^a-zA-Z0-9]/g, '')})`} />
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
