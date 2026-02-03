import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface PaginationProps {
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  className?: string;
}

export function Pagination({ 
  currentPage, 
  totalPages, 
  onPageChange,
  className,
}: PaginationProps) {
  if (totalPages <= 1) return null;

  const pages: (number | 'ellipsis')[] = [];
  
  // Always show first page
  pages.push(1);

  // Calculate range around current page
  const start = Math.max(2, currentPage - 1);
  const end = Math.min(totalPages - 1, currentPage + 1);

  if (start > 2) {
    pages.push('ellipsis');
  }

  for (let i = start; i <= end; i++) {
    pages.push(i);
  }

  if (end < totalPages - 1) {
    pages.push('ellipsis');
  }

  // Always show last page
  if (totalPages > 1) {
    pages.push(totalPages);
  }

  return (
    <div className={cn("flex items-center justify-center gap-1", className)}>
      <Button
        size="icon"
        variant="ghost"
        onClick={() => onPageChange(1)}
        disabled={currentPage === 1}
        className="h-8 w-8"
      >
        <ChevronsLeft className="w-4 h-4" />
      </Button>
      
      <Button
        size="icon"
        variant="ghost"
        onClick={() => onPageChange(currentPage - 1)}
        disabled={currentPage === 1}
        className="h-8 w-8"
      >
        <ChevronLeft className="w-4 h-4" />
      </Button>

      {pages.map((page, index) => 
        page === 'ellipsis' ? (
          <span key={`ellipsis-${index}`} className="px-2 text-muted-foreground">
            …
          </span>
        ) : (
          <Button
            key={page}
            size="icon"
            variant={page === currentPage ? 'default' : 'ghost'}
            onClick={() => onPageChange(page)}
            className="h-8 w-8"
          >
            {page}
          </Button>
        )
      )}

      <Button
        size="icon"
        variant="ghost"
        onClick={() => onPageChange(currentPage + 1)}
        disabled={currentPage === totalPages}
        className="h-8 w-8"
      >
        <ChevronRight className="w-4 h-4" />
      </Button>

      <Button
        size="icon"
        variant="ghost"
        onClick={() => onPageChange(totalPages)}
        disabled={currentPage === totalPages}
        className="h-8 w-8"
      >
        <ChevronsRight className="w-4 h-4" />
      </Button>
    </div>
  );
}
