import { Link } from 'react-router-dom';
import { Disc3, PlayCircle } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { useAuth } from '@/contexts/AuthContext';
import { cn } from '@/lib/utils';

interface LibraryEmptyStateProps {
  className?: string;
  title?: string;
  description?: string;
  actionLabel?: string | null;
  onAction?: (() => void) | null;
}

export function LibraryEmptyState({ className, title, description, actionLabel, onAction }: LibraryEmptyStateProps) {
  const { isAdmin } = useAuth();
  const resolvedTitle = title || 'Library is empty';
  const resolvedDescription = description || 'No artists, albums, or tracks are indexed yet. Run your first scan to populate the library.';
  const showScanCta = !actionLabel;

  return (
    <Card className={cn('border-dashed border-border/70 bg-muted/20', className)}>
      <CardContent className="px-6 py-10 text-center space-y-4">
        <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-full bg-primary/10">
          <Disc3 className="h-7 w-7 text-primary" />
        </div>
        <div className="space-y-1.5">
          <h2 className="text-lg font-semibold text-foreground">{resolvedTitle}</h2>
          <p className="text-sm text-muted-foreground">{resolvedDescription}</p>
        </div>
        {actionLabel && onAction ? (
          <div className="flex items-center justify-center">
            <Button type="button" className="gap-2" onClick={onAction}>
              <Disc3 className="h-4 w-4" />
              {actionLabel}
            </Button>
          </div>
        ) : showScanCta && isAdmin ? (
          <div className="flex items-center justify-center">
            <Button asChild className="gap-2">
              <Link to="/scan">
                <PlayCircle className="h-4 w-4" />
                Start first scan
              </Link>
            </Button>
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">Ask an administrator to launch the first scan.</p>
        )}
      </CardContent>
    </Card>
  );
}
