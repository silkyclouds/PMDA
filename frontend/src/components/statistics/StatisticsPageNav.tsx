import { BarChart3, Database, Headphones } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import type { ReactNode } from 'react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

type StatisticsPage = 'scan' | 'listening' | 'library';

interface StatisticsPageNavProps {
  active: StatisticsPage;
  className?: string;
  children?: ReactNode;
}

export function StatisticsPageNav({ active, className, children }: StatisticsPageNavProps) {
  const navigate = useNavigate();

  return (
    <div className={cn('flex flex-wrap items-center justify-end gap-2', className)}>
      <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-muted/30 p-1">
        <Button
          variant={active === 'scan' ? 'secondary' : 'ghost'}
          size="sm"
          className="gap-2"
          onClick={() => navigate('/statistics')}
        >
          <BarChart3 className="h-4 w-4" />
          Scan statistics
        </Button>
        <Button
          variant={active === 'listening' ? 'secondary' : 'ghost'}
          size="sm"
          className="gap-2"
          onClick={() => navigate('/statistics/listening')}
        >
          <Headphones className="h-4 w-4" />
          Listening statistics
        </Button>
        <Button
          variant={active === 'library' ? 'secondary' : 'ghost'}
          size="sm"
          className="gap-2"
          onClick={() => navigate('/statistics/library')}
        >
          <Database className="h-4 w-4" />
          Library statistics
        </Button>
      </div>
      {children}
    </div>
  );
}
